from typing import NamedTuple
from datetime import datetime

from archive.v1.src.pipeline.components.scene_state.events._old_3.subprocessors.base.base_generator import ProcessingSystemState
from archive.v1.src.pipeline.components.scene_state.events._old_3.pipeline.events import EventsWrapper, FrameMetadataForEvents, EventsContainer
from tram_analytics.v1.pipeline.components.scene_state.events.pipeline.events_input import EventsInputData
from archive.v1.src.pipeline.components.scene_state.events._old_3.subprocessors.canonical.events import CanonicalEventsContainer
from archive.v1.src.pipeline.components.scene_state.events._old_3.subprocessors.canonical.generator import CanonicalEventGenerator_Old
from archive.v1.src.pipeline.components.scene_state.events._old_3.subprocessors.motion_global.events import MotionStatusEventsContainer
from archive.v1.src.pipeline.components.scene_state.events._old_3.subprocessors.motion_global.generator import MotionStatusEventGenerator_Old
from archive.v1.src.pipeline.components.scene_state.events._old_3.subprocessors.motion_in_zone.events import InZoneMotionEventsContainer
from archive.v1.src.pipeline.components.scene_state.events._old_3.subprocessors.motion_in_zone.generator import InZoneMotionStatusEventGenerator, InZoneMotionStatusInput
from tram_analytics.v1.pipeline.components.scene_state.config.scene_events_config import SceneEventsConfig


class PipelineSteps(NamedTuple):
    canonical: CanonicalEventGenerator_Old
    global_motion: MotionStatusEventGenerator_Old
    motion_in_zone: InZoneMotionStatusEventGenerator

class PipelineState(NamedTuple):
    prev_frame_id: str | None
    prev_frame_ts: datetime | None
    has_processed_first_frame: bool

class EventPipeline:

    def __init__(self, config: SceneEventsConfig, *, camera_id: str) -> None:
        self._camera_id: str = camera_id
        self._steps: PipelineSteps = PipelineSteps(
            canonical=CanonicalEventGenerator_Old(),
            global_motion=MotionStatusEventGenerator_Old(config.stationary_global),
            motion_in_zone=InZoneMotionStatusEventGenerator()
        )
        self._state: PipelineState = PipelineState(has_processed_first_frame=False,
                                                   prev_frame_ts=None,
                                                   prev_frame_id=None)
        self._ended_processing: bool = False

    def _check_input(self, input_data: EventsInputData) -> None:
        # camera id must stay the same
        cur_cam_id: str = input_data.camera_id
        if cur_cam_id != self._camera_id:
            raise ValueError("Passed a different camera ID than the one received previously "
                             f"(expected {self._camera_id}, got {cur_cam_id})")
        # the frame timestamp must have increased
        prev_ts: datetime | None = self._state.prev_frame_ts
        cur_ts: datetime = input_data.frame_ts
        if prev_ts is not None and not prev_ts < cur_ts:
            raise ValueError("The current frame timestamp must be greater than the previous one "
                             f"(previous {prev_ts}, received current: {cur_ts})")

    def _update_state(self, input_data: EventsInputData) -> None:
        # call AFTER processing
        self._state = PipelineState(has_processed_first_frame=True,
                                    prev_frame_ts=input_data.frame_ts,
                                    prev_frame_id=input_data.frame_id)

    def update_and_get_events(self, input_data: EventsInputData) -> EventsContainer:
        if self._ended_processing:
            raise ValueError("This pipeline has ended processing")
        self._check_input(input_data)
        system_state: ProcessingSystemState = ProcessingSystemState(
            cur_frame_ts=input_data.frame_ts,
            is_first_frame=not self._state.has_processed_first_frame
        )

        canonical_events: CanonicalEventsContainer = self._steps.canonical.update_and_get_events(
            input_data, system_state=system_state
        )
        global_motion_events: MotionStatusEventsContainer = self._steps.global_motion.update_and_get_events(
            canonical_events.speeds, system_state=system_state
        )
        motion_in_zone_input: InZoneMotionStatusInput = InZoneMotionStatusInput(
            lifetime_events=canonical_events.lifetime,
            global_motion_events=global_motion_events,
            zone_occupancy_events=canonical_events.zone_transit
        )
        motion_in_zone_events: InZoneMotionEventsContainer = self._steps.motion_in_zone.update_and_get_events(
            motion_in_zone_input, system_state=system_state
        )
        wrapper: EventsWrapper = EventsWrapper(
            canonical=canonical_events,
            global_motion=global_motion_events,
            motion_in_zone=motion_in_zone_events
        )
        metadata: FrameMetadataForEvents = FrameMetadataForEvents(
            camera_id=input_data.camera_id,
            frame_id=input_data.frame_id,
            frame_ts=input_data.frame_ts,
            is_processing_start=not self._state.has_processed_first_frame,
            is_processing_end=False
        )
        container: EventsContainer = EventsContainer(metadata=metadata,
                                                     pipeline_steps=wrapper)

        self._update_state(input_data)

        return container

    def end_processing_and_get_events(self) -> EventsContainer:
        if self._ended_processing:
            raise ValueError("This pipeline has ended processing")
        canonical_events: CanonicalEventsContainer = (
            self._steps.canonical.end_processing_and_get_events_with_truncation()
        )
        global_motion_events: MotionStatusEventsContainer = (
            self._steps.global_motion.end_processing_and_get_events_with_truncation()
        )
        motion_in_zone_events: InZoneMotionEventsContainer = (
            self._steps.motion_in_zone.end_processing_and_get_events_with_truncation()
        )
        wrapper: EventsWrapper = EventsWrapper(
            canonical=canonical_events,
            global_motion=global_motion_events,
            motion_in_zone=motion_in_zone_events
        )
        metadata: FrameMetadataForEvents = FrameMetadataForEvents(
            camera_id=self._camera_id,
            frame_id=self._state.prev_frame_id,
            frame_ts=self._state.prev_frame_ts,
            is_processing_start=False,
            is_processing_end=True
        )
        container: EventsContainer = EventsContainer(metadata=metadata, pipeline_steps=wrapper)

        self._ended_processing = True

        return container
