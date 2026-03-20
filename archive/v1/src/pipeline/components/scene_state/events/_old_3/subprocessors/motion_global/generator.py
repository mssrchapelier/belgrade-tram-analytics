from typing import Set, List, Dict, NamedTuple, Iterable, override, Type
from datetime import datetime

from tram_analytics.v1.models.components.scene_state.events.base import generate_event_uuid
from archive.v1.src.pipeline.components.scene_state.events._old_3.subprocessors.base.base_generator import (
    BaseFinalEventGenerator, ProcessingSystemState
)
from tram_analytics.v1.models.common_types import MotionStatus, SpeedType
from tram_analytics.v1.models.components.scene_state.events.subprocessors.speed import SpeedUpdateEvent
from src.v1.events.subprocessors.motion_global.updates.momentary.config import MotionStatusDeterminationSettings
from archive.v1.src.pipeline.components.scene_state.events._old_3.subprocessors.motion_global.events import (
    MotionStatusUpdate,
    MotionStatusEventsContainer
)
from src.v1.events.subprocessors.motion_global.periods.confirmed.events import StationaryStartEvent, StationaryEndEvent, \
    MovingStartEvent, MovingEndEvent, MotionStatusStartEventsContainer, MotionStatusEndEventsContainer, \
    GlobalMotionStatusPeriodBoundaryEventsContainer


class VehicleStatusesForChangeEventCalculation(NamedTuple):
    vehicle_id: str
    prev_confirmed: MotionStatus | None
    cur_confirmed: MotionStatus

class VehicleMotionStatuses(NamedTuple):
    momentary: MotionStatus
    confirmed: MotionStatus


class MotionStatusEventGenerator_Old(BaseFinalEventGenerator[List[SpeedUpdateEvent], MotionStatusEventsContainer]):

    def __init__(self, config: MotionStatusDeterminationSettings):
        super().__init__()
        self._config: MotionStatusDeterminationSettings = config
        self._vehicle_id_to_confirmed_status: Dict[str, VehicleMotionStatuses] = dict()
        # self._prev_state_snapshot: MotionStatusEventGeneratorStateSnapshot | None = None
        # self._state: MotionStatusEventGeneratorState = MotionStatusEventGeneratorState()

    @override
    @classmethod
    def _get_container_class(cls) -> Type[MotionStatusEventsContainer]:
        return MotionStatusEventsContainer

    @override
    def _clear_own_state(self) -> None:
        # TODO: possibly also clear config
        # (but needs to be settable to None in that case; more runtime state validation)
        self._vehicle_id_to_confirmed_status.clear()

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

    @staticmethod
    def _compute_confirmed_status(
            *, prev_confirmed: MotionStatus, cur_momentary: MotionStatus
    ) -> MotionStatus:
        if cur_momentary is MotionStatus.UNDEFINED:
            # current momentary status is undefined -- does not change the previous confirmed status
            return prev_confirmed
        # current is defined -- assign the it as the confirmed one
        return cur_momentary

    def _build_confirmed_status_final_end_events(
            self, event_ts: datetime, *, truncate_events: bool
    ) -> GlobalMotionStatusPeriodBoundaryEventsContainer:
        stationary_end: List[StationaryEndEvent] = []
        moving_end: List[MovingEndEvent] = []
        for vehicle_id, vehicle_state in self._vehicle_id_to_confirmed_status.items(): # type: str, VehicleMotionStatuses
            match vehicle_state.confirmed:
                case MotionStatus.UNDEFINED:
                    pass
                case MotionStatus.STATIONARY:
                    # emit stationary end event
                    stationary_end.append(StationaryEndEvent(event_id=generate_event_uuid(),
                                                             vehicle_id=vehicle_id,
                                                             event_ts=event_ts,
                                                             truncated=truncate_events))
                case MotionStatus.MOVING:
                    moving_end.append(MovingEndEvent(event_id=generate_event_uuid(),
                                                     vehicle_id=vehicle_id,
                                                     event_ts=event_ts,
                                                     truncated=truncate_events))
                case _:
                    raise RuntimeError(f"Unknown motion (confirmed) status: {vehicle_state.confirmed}")
        end_events: MotionStatusEndEventsContainer = MotionStatusEndEventsContainer(
            stationary=stationary_end, moving=moving_end
        )
        start_events: MotionStatusStartEventsContainer = (
            MotionStatusStartEventsContainer.create_empty_container()
        )
        change_events_container: GlobalMotionStatusPeriodBoundaryEventsContainer = GlobalMotionStatusPeriodBoundaryEventsContainer(
            start=start_events, end=end_events
        )
        return change_events_container

    def _build_confirmed_status_change_events(
            self, items: Iterable[VehicleStatusesForChangeEventCalculation],
            *, system_state: ProcessingSystemState
    ) -> GlobalMotionStatusPeriodBoundaryEventsContainer:

        stationary_start: List[StationaryStartEvent] = []
        stationary_end: List[StationaryEndEvent] = []
        moving_start: List[MovingStartEvent] = []
        moving_end: List[MovingEndEvent] = []

        cur_frame_ts: datetime = system_state.cur_frame_ts
        is_first_frame: bool = system_state.is_first_frame

        for item in items: # type: VehicleStatusesForChangeEventCalculation
            vehicle_id: str = item.vehicle_id
            prev_confirmed: MotionStatus | None = item.prev_confirmed
            cur_confirmed: MotionStatus = item.cur_confirmed

            # NOTE: prev_confirmed is None if this frame is the vehicle's first
            if not (
                    (vehicle_id not in self._vehicle_id_to_confirmed_status and prev_confirmed is None)
                    or (vehicle_id in self._vehicle_id_to_confirmed_status and prev_confirmed is not None)
            ):
                raise ValueError("Incompatible arguments: prev_confirmed must not be None for a vehicle "
                                 "that has appeared in the past, and must be None for one that hasn't")

            if is_first_frame and prev_confirmed is not None:
                # by design, impossible: prev_confirmed cannot have been generated on the first frame
                raise ValueError("Inconsistent state: is_first_frame is True, "
                                 "but the previous confirmed motion status has already been defined")

            if prev_confirmed is not MotionStatus.STATIONARY and cur_confirmed is MotionStatus.STATIONARY:
                # emit stationary start
                stationary_start.append(StationaryStartEvent(event_id=generate_event_uuid(),
                                                             vehicle_id=vehicle_id,
                                                             event_ts=cur_frame_ts,
                                                             truncated=is_first_frame))
            if prev_confirmed is MotionStatus.STATIONARY and cur_confirmed is MotionStatus.MOVING:
                # emit stationary end
                stationary_end.append(StationaryEndEvent(event_id=generate_event_uuid(),
                                                         vehicle_id=vehicle_id,
                                                         event_ts=cur_frame_ts,
                                                         truncated=False))
            if prev_confirmed is not MotionStatus.MOVING and cur_confirmed is MotionStatus.MOVING:
                # emit moving start
                moving_start.append(MovingStartEvent(event_id=generate_event_uuid(),
                                                     vehicle_id=vehicle_id,
                                                     event_ts=cur_frame_ts,
                                                     truncated=False))
            if prev_confirmed is MotionStatus.MOVING and cur_confirmed is MotionStatus.STATIONARY:
                # emit moving end
                moving_end.append(MovingEndEvent(event_id=generate_event_uuid(),
                                                 vehicle_id=vehicle_id,
                                                 event_ts=cur_frame_ts,
                                                 truncated=False))

        start_events: MotionStatusStartEventsContainer = MotionStatusStartEventsContainer(
            stationary=stationary_start, moving=moving_start
        )
        end_events: MotionStatusEndEventsContainer = MotionStatusEndEventsContainer(
            stationary=stationary_end, moving=moving_end
        )
        change_events_container: GlobalMotionStatusPeriodBoundaryEventsContainer = GlobalMotionStatusPeriodBoundaryEventsContainer(
            start=start_events, end=end_events
        )
        return change_events_container

    @override
    def _get_events_for_end_of_processing(
            self, *, event_ts: datetime, truncate_events: bool
    ) -> MotionStatusEventsContainer:
        """
        Must be called after processing the last frame in a stream to produce end events for alive vehicles
        (if there are any that are moving or stationary, not doing so will result in an unended period).
        """
        confirmed_changes_events: GlobalMotionStatusPeriodBoundaryEventsContainer = self._build_confirmed_status_final_end_events(
            event_ts=event_ts, truncate_events=truncate_events
        )
        container: MotionStatusEventsContainer = MotionStatusEventsContainer(
            status_updates=[],
            changes_in_confirmed_status=confirmed_changes_events
        )
        return container

    @override
    def _update_and_get_events(
            self, speed_events: List[SpeedUpdateEvent], *, system_state: ProcessingSystemState
    ) -> MotionStatusEventsContainer:

        alive_vehicle_ids: Set[str] = set()

        status_updates: List[MotionStatusUpdate] = []

        mappings_for_state_update: Dict[str, VehicleMotionStatuses] = dict()
        mappings_for_changes_calculation: List[VehicleStatusesForChangeEventCalculation] = []

        # calculate the current momentary and confirmed statuses, create the status update events
        for speed_event in speed_events:  # type: SpeedUpdateEvent
            vehicle_id: str = speed_event.vehicle_id
            # add to alive ids
            alive_vehicle_ids.add(vehicle_id)
            # calculate the current momentary status, create update event
            cur_momentary_status: MotionStatus = self._compute_momentary_status(
                speed_raw=speed_event.speeds.raw,
                speed_smoothed=speed_event.speeds.smoothed,
                is_matched=speed_event.is_matched
            )
            # calculate the current confirmed status, create update event
            prev_confirmed_status: MotionStatus | None = (
                self._vehicle_id_to_confirmed_status[vehicle_id].confirmed
                if vehicle_id in self._vehicle_id_to_confirmed_status
                else None
            )
            cur_confirmed_status: MotionStatus = (
                self._compute_confirmed_status(prev_confirmed=prev_confirmed_status,
                                               cur_momentary=cur_momentary_status)
                if prev_confirmed_status is not None
                else cur_momentary_status
            )

            status_updates.append(MotionStatusUpdate(event_id=generate_event_uuid(),
                                                     vehicle_id=vehicle_id,
                                                     momentary=cur_momentary_status,
                                                     confirmed=cur_confirmed_status,
                                                     event_ts=system_state.cur_frame_ts,
                                                     truncated=system_state.is_first_frame))

            # create a mapping for state update
            mappings_for_state_update[vehicle_id] = VehicleMotionStatuses(momentary=cur_momentary_status,
                                                                          confirmed=cur_confirmed_status)
            # create and add a mapping for status change calculation function
            mapping: VehicleStatusesForChangeEventCalculation = VehicleStatusesForChangeEventCalculation(
                vehicle_id=vehicle_id, prev_confirmed=prev_confirmed_status, cur_confirmed=cur_confirmed_status
            )
            mappings_for_changes_calculation.append(mapping)
        # confirmed status change events
        confirmed_changes_events: GlobalMotionStatusPeriodBoundaryEventsContainer = self._build_confirmed_status_change_events(
            mappings_for_changes_calculation, system_state=system_state
        )
        # create the final container
        container: MotionStatusEventsContainer = MotionStatusEventsContainer(
            status_updates=status_updates,
            changes_in_confirmed_status=confirmed_changes_events
        )

        prev_alive_vehicle_ids: Set[str] = set(self._vehicle_id_to_confirmed_status.keys())
        dead_vehicle_ids: Set[str] = set.difference(prev_alive_vehicle_ids, alive_vehicle_ids)
        # remove dead vehicle ID keys from the state
        for dead_id in dead_vehicle_ids: # type: str
            self._vehicle_id_to_confirmed_status.pop(dead_id)
        # update the state with new momentary and confirmed values
        self._vehicle_id_to_confirmed_status.update(mappings_for_state_update)

        return container
