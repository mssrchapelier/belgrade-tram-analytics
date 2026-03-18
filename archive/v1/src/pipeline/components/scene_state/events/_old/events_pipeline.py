from typing import NamedTuple
from datetime import datetime

from pydantic import BaseModel

from tram_analytics.v1.pipeline.components.scene_state.events.pipeline.events_input import EventsInputData
from archive.v1.src.pipeline.components.scene_state.events._old_3.subprocessors.canonical.events import CanonicalEventsContainer
from archive.v1.src.pipeline.components.scene_state.events._old_3.subprocessors.canonical.generator import CanonicalEventGenerator_Old
from archive.v1.src.pipeline.components.scene_state.events._old_2.motion_global.events import StationaryEventsContainer_Old
from archive.v1.src.pipeline.components.scene_state.events._old_3.subprocessors.motion_global.generator import StationaryEventGenerator_Old
from archive.v1.src.pipeline.components.scene_state.events._old_2.motion_in_zone.events import InZoneStationaryEventsContainer
from archive.v1.src.pipeline.components.scene_state.events._old_2.motion_in_zone.generator import StationaryInZoneEventGenerator
from tram_analytics.v1.pipeline.components.scene_state.config.scene_events_config import SceneEventsConfig

class EventPipelineStepOutputWrapper(BaseModel):
    canonical: CanonicalEventsContainer
    stationary_global: StationaryEventsContainer_Old
    stationary_in_zone: InZoneStationaryEventsContainer

    # TODO: more advanced validation
    # (1) impossible combinations:
    # - for the same vehicle id:
    #   * lifetime end and stationary global start
    #   * lifetime end and stationary in zone start
    # - for the same zone id:
    #   * zone exit and in-zone stationary start
    # (2) all vehicle ids from stationary global, stationary in zone -- must be present in speeds

class EventsContainer(BaseModel):

    camera_id: str

    frame_id: str
    frame_ts: datetime

    pipeline_steps: EventPipelineStepOutputWrapper

class PipelineSteps(NamedTuple):
    canonical: CanonicalEventGenerator_Old
    stationary_global: StationaryEventGenerator_Old
    stationary_in_zone: StationaryInZoneEventGenerator

class PipelineState(NamedTuple):
    camera_id: str
    prev_frame_ts: datetime


class EventPipeline:

    def __init__(self, config: SceneEventsConfig) -> None:
        self._steps: PipelineSteps = PipelineSteps(
            canonical=CanonicalEventGenerator_Old(),
            stationary_global=StationaryEventGenerator_Old(config.stationary_global),
            stationary_in_zone=StationaryInZoneEventGenerator()
        )
        self._state: PipelineState | None = None

    def _check_input(self, input_data: EventsInputData) -> None:
        if self._state is None:
            return
        # camera id must stay the same
        expected_cam_id: str = self._state.camera_id
        cur_cam_id: str = input_data.camera_id
        if cur_cam_id != expected_cam_id:
            raise ValueError("Passed a different camera ID than the one received previously "
                             f"(expected {expected_cam_id}, got {cur_cam_id})")
        # the frame timestamp must have increased
        prev_ts: datetime = self._state.prev_frame_ts
        cur_ts: datetime = input_data.frame_ts
        if not prev_ts < cur_ts:
            raise ValueError("The current frame timestamp must be greater than the previous one "
                             f"(previous {prev_ts}, received current: {cur_ts})")

    def _update_state(self, input_data: EventsInputData) -> None:
        # call AFTER processing
        self._state = PipelineState(camera_id=input_data.camera_id,
                                    prev_frame_ts=input_data.frame_ts)

    def update_and_get_events(self, input_data: EventsInputData) -> EventsContainer:

        self._check_input(input_data)

        canonical_events: CanonicalEventsContainer = self._steps.canonical.update_and_get_events(input_data)
        stationary_global_events: StationaryEventsContainer_Old = self._steps.stationary_global.update_and_get_events(
            canonical_events.speeds
        )
        stationary_in_zone_events: InZoneStationaryEventsContainer = self._steps.stationary_in_zone.update_and_get_events(
            lifetime_events=canonical_events.lifetime,
            zone_occupancy_events=canonical_events.zone_transit,
            global_stationary_events=stationary_global_events
        )
        wrapper: EventPipelineStepOutputWrapper = EventPipelineStepOutputWrapper(
            canonical=canonical_events,
            stationary_global=stationary_global_events,
            stationary_in_zone=stationary_in_zone_events
        )
        container: EventsContainer = EventsContainer(
            camera_id=input_data.camera_id,
            frame_id=input_data.frame_id,
            frame_ts=input_data.frame_ts,
            pipeline_steps=wrapper
        )

        self._update_state(input_data)

        return container

