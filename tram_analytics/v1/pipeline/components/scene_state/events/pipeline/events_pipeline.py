from datetime import datetime
from typing import NamedTuple

from tram_analytics.v1.models.components.scene_state.events.scene_events import SceneEventsWrapper, \
    FrameMetadataForEvents, EventsContainer
# --- event containers ---
from tram_analytics.v1.models.components.scene_state.events.subprocessors.lifetime import \
    VehiclesLifetimeEventsContainer
from tram_analytics.v1.models.components.scene_state.events.subprocessors.motion_status.motion_global.motion_global_events import \
    GlobalMotionStatusEventsContainer
from tram_analytics.v1.models.components.scene_state.events.subprocessors.motion_status.motion_global.periods.confirmed import (
    GlobalMotionStatusPeriodBoundaryEventsContainer
)
from tram_analytics.v1.models.components.scene_state.events.subprocessors.motion_status.motion_global.updates.confirmed import (
    ConfirmedMotionStatusUpdatesContainer
)
from tram_analytics.v1.models.components.scene_state.events.subprocessors.motion_status.motion_global.updates.momentary import (
    MomentaryMotionStatusUpdatesContainer
)
from tram_analytics.v1.models.components.scene_state.events.subprocessors.motion_status.motion_global.updates.motion_global_updates import (
    MotionStatusUpdatesContainer
)
from tram_analytics.v1.models.components.scene_state.events.subprocessors.motion_status.motion_in_zone.periods.confirmed import (
    InZoneMotionStatusPeriodBoundaryEventsContainer
)
from tram_analytics.v1.models.components.scene_state.events.subprocessors.motion_status.motion_status import \
    MotionStatusEventsContainer
from tram_analytics.v1.models.components.scene_state.events.subprocessors.speed import SpeedUpdatesContainer
from tram_analytics.v1.models.components.scene_state.events.subprocessors.zone_occupancy import \
    ZoneOccupancyEventsContainer
from tram_analytics.v1.models.components.scene_state.events.subprocessors.zone_transit import \
    ZoneTransitEventsContainer
from tram_analytics.v1.pipeline.components.scene_state.config.scene_events_config import SceneEventsConfig
from tram_analytics.v1.pipeline.components.scene_state.events.pipeline.events_input import EventsInputData
from tram_analytics.v1.pipeline.components.scene_state.events.subprocessors.base.base_generator import \
    ProcessingSystemState
# --- generators ---
from tram_analytics.v1.pipeline.components.scene_state.events.subprocessors.lifetime import LifetimeEventGenerator
from tram_analytics.v1.pipeline.components.scene_state.events.subprocessors.motion_status.motion_global.periods.confirmed.generator import (
    MotionStatusPeriodGenerator, MotionStatusPeriodGeneratorInput
)
from tram_analytics.v1.pipeline.components.scene_state.events.subprocessors.motion_status.motion_global.updates.confirmed import (
    ConfirmedMotionStatusUpdateGenerator, ConfirmedMotionStatusUpdatesInput
)
from tram_analytics.v1.pipeline.components.scene_state.events.subprocessors.motion_status.motion_global.updates.momentary import (
    MomentaryMotionStatusUpdateGenerator
)
from tram_analytics.v1.pipeline.components.scene_state.events.subprocessors.motion_status.motion_in_zone.periods.confirmed.generator import (
    InZoneMotionStatusPeriodBoundaryEventGenerator, InZoneMotionStatusPeriodBoundaryEventGeneratorInput
)
from tram_analytics.v1.pipeline.components.scene_state.events.subprocessors.speed import SpeedUpdateGenerator
from tram_analytics.v1.pipeline.components.scene_state.events.subprocessors.zone_occupancy import \
    ZoneOccupancyEventGenerator
from tram_analytics.v1.pipeline.components.scene_state.events.subprocessors.zone_transit import \
    ZoneTransitEventGenerator


# --- containers for pipeline steps ---

class GlobalMotionStatusUpdatesSteps(NamedTuple):
    momentary: MomentaryMotionStatusUpdateGenerator
    confirmed: ConfirmedMotionStatusUpdateGenerator

class GlobalMotionStatusSteps(NamedTuple):
    updates: GlobalMotionStatusUpdatesSteps
    periods_for_confirmed: MotionStatusPeriodGenerator

class MotionStatusSteps(NamedTuple):
    motion_global: GlobalMotionStatusSteps
    motion_in_zone_periods_for_confirmed: InZoneMotionStatusPeriodBoundaryEventGenerator

class PipelineSteps(NamedTuple):
    lifetime: LifetimeEventGenerator
    speed: SpeedUpdateGenerator
    zone_transit: ZoneTransitEventGenerator
    zone_occupancy: ZoneOccupancyEventGenerator
    motion_status: MotionStatusSteps

class EventsExplodedContainer(NamedTuple):
    # all events produced by pipeline steps without nesting
    lifetime: VehiclesLifetimeEventsContainer
    speeds: SpeedUpdatesContainer
    zone_transit: ZoneTransitEventsContainer
    motion_status_global_updates_momentary: MomentaryMotionStatusUpdatesContainer
    zone_occupancy: ZoneOccupancyEventsContainer
    motion_status_global_updates_confirmed: ConfirmedMotionStatusUpdatesContainer
    motion_status_global_period_confirmed: GlobalMotionStatusPeriodBoundaryEventsContainer
    motion_status_inzone_period_confirmed: InZoneMotionStatusPeriodBoundaryEventsContainer

def _build_event_wrapper_from_exploded(exploded: EventsExplodedContainer) -> SceneEventsWrapper:
    # building the wrapper
    motion_status_global_updates: MotionStatusUpdatesContainer = (
        MotionStatusUpdatesContainer(momentary=exploded.motion_status_global_updates_momentary,
                                     confirmed=exploded.motion_status_global_updates_confirmed)
    )
    motion_status_global: GlobalMotionStatusEventsContainer = (
        GlobalMotionStatusEventsContainer(
            updates=motion_status_global_updates,
            period_boundary_events_for_confirmed_status=exploded.motion_status_global_period_confirmed
        )
    )
    motion_status: MotionStatusEventsContainer = (
        MotionStatusEventsContainer(motion_global=motion_status_global,
                                    motion_in_zone=exploded.motion_status_inzone_period_confirmed)
    )
    wrapper: SceneEventsWrapper = SceneEventsWrapper(vehicle_lifetime=exploded.lifetime,
                                                     speeds=exploded.speeds,
                                                     zone_transit=exploded.zone_transit,
                                                     zone_occupancy=exploded.zone_occupancy,
                                                     motion_status=motion_status)
    return wrapper

class PipelineState(NamedTuple):
    prev_frame_id: str | None
    prev_frame_ts: datetime | None
    has_processed_first_frame: bool

class EventPipeline:

    def __init__(self, config: SceneEventsConfig, *, camera_id: str) -> None:
        self._camera_id: str = camera_id
        self._steps: PipelineSteps = self._init_pipeline_steps(camera_id=camera_id,
                                                               config=config)
        self._state: PipelineState = PipelineState(has_processed_first_frame=False,
                                                   prev_frame_ts=None,
                                                   prev_frame_id=None)
        self._ended_processing: bool = False

    @staticmethod
    def _init_pipeline_steps(*, camera_id: str, config: SceneEventsConfig) -> PipelineSteps:
        return PipelineSteps(
            lifetime=LifetimeEventGenerator(camera_id),
            speed=SpeedUpdateGenerator(camera_id),
            zone_transit=ZoneTransitEventGenerator(camera_id),
            zone_occupancy=ZoneOccupancyEventGenerator(camera_id),
            motion_status=MotionStatusSteps(
                motion_global=GlobalMotionStatusSteps(
                    updates=GlobalMotionStatusUpdatesSteps(
                        momentary=MomentaryMotionStatusUpdateGenerator(camera_id=camera_id,
                                                                       config=config.stationary_global),
                        confirmed=ConfirmedMotionStatusUpdateGenerator(camera_id)
                    ),
                    periods_for_confirmed=MotionStatusPeriodGenerator(camera_id)
                ),
                motion_in_zone_periods_for_confirmed=InZoneMotionStatusPeriodBoundaryEventGenerator(camera_id)
            )
        )

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

    def _update_and_get_events(self, input_data: EventsInputData) -> SceneEventsWrapper:

        system_state: ProcessingSystemState = ProcessingSystemState(
            cur_frame_ts=input_data.frame_ts,
            is_first_frame=not self._state.has_processed_first_frame
        )

        # --- 1 ---
        # events computed directly from input
        lifetime: VehiclesLifetimeEventsContainer = self._steps.lifetime.update_and_get_events(
            input_data, system_state=system_state
        )
        speeds: SpeedUpdatesContainer = self._steps.speed.update_and_get_events(
            input_data, system_state=system_state
        )
        zone_transit: ZoneTransitEventsContainer = self._steps.zone_transit.update_and_get_events(
            input_data, system_state=system_state
        )
        motion_status_global_updates_momentary: MomentaryMotionStatusUpdatesContainer = (
            self._steps.motion_status.motion_global.updates.momentary.update_and_get_events(
                input_data, system_state=system_state
            )
        )

        # --- 2 ---
        # derived events
        zone_occupancy: ZoneOccupancyEventsContainer = self._steps.zone_occupancy.update_and_get_events(
            zone_transit, system_state=system_state
        )

        motion_status_global_updates_confirmed: ConfirmedMotionStatusUpdatesContainer = (
            self._steps.motion_status.motion_global.updates.confirmed.update_and_get_events(
                ConfirmedMotionStatusUpdatesInput(
                    lifetime_events=lifetime,
                    momentary_motion_status_updates=motion_status_global_updates_momentary
                ),
                system_state=system_state
            )
        )

        motion_status_global_period_confirmed: GlobalMotionStatusPeriodBoundaryEventsContainer = (
            self._steps.motion_status.motion_global.periods_for_confirmed.update_and_get_events(
                MotionStatusPeriodGeneratorInput(
                    lifetime_events=lifetime,
                    confirmed_motion_status_updates=motion_status_global_updates_confirmed
                ),
                system_state=system_state
            )
        )

        motion_status_inzone_period_confirmed: InZoneMotionStatusPeriodBoundaryEventsContainer = (
            self._steps.motion_status.motion_in_zone_periods_for_confirmed.update_and_get_events(
                InZoneMotionStatusPeriodBoundaryEventGeneratorInput(
                    lifetime_events=lifetime,
                    global_motion_status_updates=motion_status_global_updates_confirmed,
                    zone_transit_events=zone_transit
                ),
                system_state=system_state
            )
        )

        # building the wrapper
        wrapper: SceneEventsWrapper = _build_event_wrapper_from_exploded(
            EventsExplodedContainer(lifetime,
                                    speeds,
                                    zone_transit,
                                    motion_status_global_updates_momentary,
                                    zone_occupancy,
                                    motion_status_global_updates_confirmed,
                                    motion_status_global_period_confirmed,
                                    motion_status_inzone_period_confirmed)
        )
        return wrapper

    def _end_processing_and_get_events(self) -> SceneEventsWrapper:
        # --- 1 ---
        # events computed directly from input
        lifetime: VehiclesLifetimeEventsContainer = self._steps.lifetime.end_processing_and_get_events_with_truncation()
        speeds: SpeedUpdatesContainer = self._steps.speed.end_processing_and_get_events_with_truncation()
        zone_transit: ZoneTransitEventsContainer = self._steps.zone_transit.end_processing_and_get_events_with_truncation()
        motion_status_global_updates_momentary: MomentaryMotionStatusUpdatesContainer = (
            self._steps.motion_status.motion_global.updates.momentary.end_processing_and_get_events_with_truncation()
        )

        # --- 2 ---
        # derived events
        zone_occupancy: ZoneOccupancyEventsContainer = (
            self._steps.zone_occupancy.end_processing_and_get_events_with_truncation()
        )

        motion_status_global_updates_confirmed: ConfirmedMotionStatusUpdatesContainer = (
            self._steps.motion_status.motion_global.updates.confirmed.end_processing_and_get_events_with_truncation()
        )

        motion_status_global_period_confirmed: GlobalMotionStatusPeriodBoundaryEventsContainer = (
            self._steps.motion_status.motion_global.periods_for_confirmed.end_processing_and_get_events_with_truncation()
        )

        motion_status_inzone_period_confirmed: InZoneMotionStatusPeriodBoundaryEventsContainer = (
            self._steps.motion_status.motion_in_zone_periods_for_confirmed.end_processing_and_get_events_with_truncation()
        )

        # building the wrapper
        wrapper: SceneEventsWrapper = _build_event_wrapper_from_exploded(
            EventsExplodedContainer(lifetime,
                                    speeds,
                                    zone_transit,
                                    motion_status_global_updates_momentary,
                                    zone_occupancy,
                                    motion_status_global_updates_confirmed,
                                    motion_status_global_period_confirmed,
                                    motion_status_inzone_period_confirmed)
        )
        return wrapper

    def update_and_get_events(self, input_data: EventsInputData) -> EventsContainer:
        if self._ended_processing:
            raise ValueError("This pipeline has ended processing")
        self._check_input(input_data)

        wrapper: SceneEventsWrapper = self._update_and_get_events(input_data)
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

        wrapper: SceneEventsWrapper = self._end_processing_and_get_events()
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