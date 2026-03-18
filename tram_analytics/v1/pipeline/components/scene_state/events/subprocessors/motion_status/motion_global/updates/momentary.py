from datetime import datetime
from typing import List, override, Type

from archive.v1.src.api_server.models.scene_state_settings import SpeedType
from tram_analytics.v1.models.common_types import MotionStatus
from tram_analytics.v1.models.components.scene_state.events.base import generate_event_uuid
from tram_analytics.v1.models.components.scene_state.events.subprocessors.motion_status.motion_global.updates.momentary import (
    MomentaryMotionStatusUpdate, MomentaryMotionStatusUpdatesContainer
)
from tram_analytics.v1.pipeline.components.scene_state.config.scene_events_config import \
    MotionStatusDeterminationSettings
from tram_analytics.v1.pipeline.components.scene_state.events.pipeline.events_input import EventsInputData, \
    VehicleInput
from tram_analytics.v1.pipeline.components.scene_state.events.subprocessors.base.base_generator import (
    BaseFinalEventGenerator, BaseGeneratorState, ProcessingSystemState
)


class MomentaryMotionStatusUpdateGeneratorState(BaseGeneratorState):

    @override
    def _clear_own_state(self) -> None:
        pass

class MomentaryMotionStatusUpdateGenerator(
    BaseFinalEventGenerator[
        EventsInputData, MomentaryMotionStatusUpdatesContainer, MomentaryMotionStatusUpdateGeneratorState
    ]
):

    def __init__(self, *, camera_id: str, config: MotionStatusDeterminationSettings):
        super().__init__(camera_id)
        self._config: MotionStatusDeterminationSettings = config

    @override
    @classmethod
    def _get_container_class(cls) -> Type[MomentaryMotionStatusUpdatesContainer]:
        return MomentaryMotionStatusUpdatesContainer

    @override
    @classmethod
    def _get_new_state(cls) -> MomentaryMotionStatusUpdateGeneratorState:
        return MomentaryMotionStatusUpdateGeneratorState()

    def _select_raw_or_smoothed(self,
                                *, speed_raw: float | None,
                                speed_smoothed: float | None) -> float | None:
        # choose whether to update based on the raw or smoothed speed based on the setting passed
        speed_type: SpeedType = self._config.speed_type_for_motion_status_determination
        match speed_type:
            case SpeedType.RAW:
                return speed_raw
            case SpeedType.SMOOTHED:
                return speed_smoothed
            case _:
                raise ValueError(f"Unsupported speed type for determination "
                                 f"of moving vs stationary status: {speed_type}")

    def _compute_momentary_status(self, *, speed_raw: float | None,
                                  speed_smoothed: float | None,
                                  is_matched: bool) -> MotionStatus:
        if not is_matched:
            # do not update the momentary motion status based on unmatched track states
            return MotionStatus.UNDEFINED
        speed_value_to_use: float | None = self._select_raw_or_smoothed(speed_raw=speed_raw,
                                                                  speed_smoothed=speed_smoothed)
        if speed_value_to_use is None:
            # do not update the stationary status based on undefined speed values
            return MotionStatus.UNDEFINED
        if speed_value_to_use <= self._config.is_stationary_speed_threshold_ms:
            return MotionStatus.STATIONARY
        return MotionStatus.MOVING

    @override
    def _update_and_get_events(
            self, input_data: EventsInputData, *, system_state: ProcessingSystemState
    ) -> MomentaryMotionStatusUpdatesContainer:
        events: List[MomentaryMotionStatusUpdate] = []
        for vehicle in input_data.vehicles: # type: VehicleInput
            status: MotionStatus = self._compute_momentary_status(
                speed_raw=vehicle.speeds.raw_ms,
                speed_smoothed=vehicle.speeds.smoothed_ms,
                is_matched=vehicle.is_matched
            )
            event: MomentaryMotionStatusUpdate = MomentaryMotionStatusUpdate(
                camera_id=self._camera_id,
                event_id=generate_event_uuid(),
                event_ts=system_state.cur_frame_ts,
                truncated=system_state.is_first_frame,
                vehicle_id=vehicle.vehicle_id,
                motion_status=status
            )
            events.append(event)
        container: MomentaryMotionStatusUpdatesContainer = MomentaryMotionStatusUpdatesContainer(
            updates=events
        )
        return container

    @override
    def _get_events_for_end_of_processing(self,
                                          *, event_ts: datetime,
                                          truncate_events: bool) -> MomentaryMotionStatusUpdatesContainer:
        # these are not period-related -- no end events are necessary
        return MomentaryMotionStatusUpdatesContainer.create_empty_container()