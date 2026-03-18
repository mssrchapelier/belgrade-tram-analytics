from datetime import datetime
from typing import List, Dict, Set, override, Type, Iterable, NamedTuple

from tram_analytics.v1.models.common_types import MotionStatus
from tram_analytics.v1.models.components.scene_state.events.base import generate_event_uuid, generate_period_uuid
from tram_analytics.v1.models.components.scene_state.events.subprocessors.lifetime import \
    VehiclesLifetimeEventsContainer
from tram_analytics.v1.models.components.scene_state.events.subprocessors.motion_status.motion_global.periods.confirmed import (
    StationaryStartEvent, StationaryEndEvent, MovingStartEvent, MovingEndEvent,
    GlobalMotionStatusPeriodBoundaryEventsContainer
)
from tram_analytics.v1.models.components.scene_state.events.subprocessors.motion_status.motion_global.updates.confirmed import (
    ConfirmedMotionStatusUpdate, ConfirmedMotionStatusUpdatesContainer
)
from tram_analytics.v1.pipeline.components.scene_state.events.subprocessors.base.base_generator import (
    BaseFinalEventGenerator, BaseGeneratorState, ProcessingSystemState
)
from tram_analytics.v1.pipeline.components.scene_state.events.subprocessors.motion_status.motion_global.periods.confirmed.intermediary_container import (
    MotionStatusPeriodStartIntermediaryEventsContainer, MotionStatusPeriodEndIntermediaryEventsContainer,
    MotionStatusPeriodBoundaryIntermediaryEventsContainer
)


# --- generator input ---

class MotionStatusPeriodGeneratorInput(NamedTuple):
    # Note:
    # From lifetime events, just the end events are used to determine when to remove the vehicle.
    # This is integrated so that an absence of a motion status update does not mean that the vehicle is dead,
    # and only a lifetime event declares that its existence has ceased,
    # although in the current version, motion status updates are generated for every vehicle that is alive.

    lifetime_events: VehiclesLifetimeEventsContainer
    confirmed_motion_status_updates: ConfirmedMotionStatusUpdatesContainer

# --- helpers ---

# event creation helpers (to avoid verbosity and boilerplate in the generator class)

class EventInitArguments(NamedTuple):
    camera_id: str
    event_id: str
    period_id: str
    vehicle_id: str
    event_ts: datetime
    truncated: bool

def _create_stationary_start_event(init_args: EventInitArguments) -> StationaryStartEvent:
    return StationaryStartEvent(camera_id=init_args.camera_id,
                                event_id=init_args.event_id,
                                period_id=init_args.period_id,
                                vehicle_id=init_args.vehicle_id,
                                event_ts=init_args.event_ts,
                                truncated=init_args.truncated)

def _create_stationary_end_event(init_args: EventInitArguments) -> StationaryEndEvent:
    return StationaryEndEvent(camera_id=init_args.camera_id,
                              event_id=init_args.event_id,
                              period_id=init_args.period_id,
                              vehicle_id=init_args.vehicle_id,
                              event_ts=init_args.event_ts,
                              truncated=init_args.truncated)

def _create_moving_start_event(init_args: EventInitArguments) -> MovingStartEvent:
    return MovingStartEvent(camera_id=init_args.camera_id,
                            event_id=init_args.event_id,
                            period_id=init_args.period_id,
                            vehicle_id=init_args.vehicle_id,
                            event_ts=init_args.event_ts,
                            truncated=init_args.truncated)

def _create_moving_end_event(init_args: EventInitArguments) -> MovingEndEvent:
    return MovingEndEvent(camera_id=init_args.camera_id,
                          event_id=init_args.event_id,
                          period_id=init_args.period_id,
                          vehicle_id=init_args.vehicle_id,
                          event_ts=init_args.event_ts,
                          truncated=init_args.truncated)

# --- generator state ---

class StoredPeriodInfo(NamedTuple):
    motion_status: MotionStatus
    period_id: str

class MotionStatusPeriodGeneratorState(BaseGeneratorState):

    def __init__(self) -> None:
        super().__init__()
        self._vehicle_id_to_period_info: Dict[str, StoredPeriodInfo] = dict()

    @override
    def _clear_own_state(self) -> None:
        self._vehicle_id_to_period_info.clear()

    def get_vehicle_ids(self) -> Set[str]:
        return set(self._vehicle_id_to_period_info.keys())

    # Defined actions:
    # (1) Add a vehicle and start a period for it.
    # (2) Change the motion status for an existing vehicle, starting a new period.
    # (3) Remove the vehicle (ending the period).

    def get_motion_status_for_vehicle(self, vehicle_id: str) -> MotionStatus:
        if vehicle_id not in self._vehicle_id_to_period_info:
            raise ValueError(f"Can't get motion status for vehicle {vehicle_id}: not registered")
        return self._vehicle_id_to_period_info[vehicle_id].motion_status

    def add_vehicle_and_start_period(
            self, *, vehicle_id: str, motion_status: MotionStatus, period_id: str
    ) -> None:
        if vehicle_id in self._vehicle_id_to_period_info:
            raise ValueError(f"Can't add vehicle {vehicle_id}: already registered")
        self._vehicle_id_to_period_info[vehicle_id] = StoredPeriodInfo(motion_status=motion_status,
                                                                       period_id=period_id)

    def change_motion_status_and_get_completed_period_id(
            self, *, vehicle_id: str, new_motion_status: MotionStatus, new_period_id: str
    ) -> str:
        # - validation -
        if vehicle_id not in self._vehicle_id_to_period_info:
            raise ValueError(f"Can't change motion status for vehicle {vehicle_id}: not registered "
                             f"(add first if it is the vehicle's first occurrence)")
        # get the old status and period id
        old_period_info: StoredPeriodInfo = self._vehicle_id_to_period_info[vehicle_id]
        old_motion_status: MotionStatus = old_period_info.motion_status
        if old_motion_status == new_motion_status:
            # the new motion status must be a different one
            raise ValueError("Can't change motion status for vehicle {vehicle_id}: "
                             "the new status is the same as the previous one")
        if old_motion_status is not MotionStatus.UNDEFINED and new_motion_status is MotionStatus.UNDEFINED:
            # defined -> undefined is not allowed
            raise ValueError("Can't change motion status for vehicle {vehicle_id}: "
                             "a transition from a defined status to an undefined one is not allowed")

        # change the motion status and period id
        new_period_info: StoredPeriodInfo = StoredPeriodInfo(motion_status=new_motion_status,
                                                             period_id=new_period_id)
        self._vehicle_id_to_period_info[vehicle_id] = new_period_info
        # return the completed period's id
        return old_period_info.period_id

    def remove_vehicle_and_get_completed_period_id(
            self, vehicle_id: str
    ) -> str:
        if vehicle_id not in self._vehicle_id_to_period_info:
            raise ValueError(f"Can't remove vehicle {vehicle_id}: not registered ")
        period_info: StoredPeriodInfo = self._vehicle_id_to_period_info.pop(vehicle_id)
        return period_info.period_id

# --- generator ---

class PeriodUpdateInfo(NamedTuple):
    vehicle_id: str
    motion_status: MotionStatus

class MotionStatusPeriodGenerator(
    BaseFinalEventGenerator[
        MotionStatusPeriodGeneratorInput, GlobalMotionStatusPeriodBoundaryEventsContainer,
        MotionStatusPeriodGeneratorState
    ]
):

    @override
    @classmethod
    def _get_container_class(cls) -> Type[GlobalMotionStatusPeriodBoundaryEventsContainer]:
        return GlobalMotionStatusPeriodBoundaryEventsContainer

    @override
    @classmethod
    def _get_new_state(cls) -> MotionStatusPeriodGeneratorState:
        return MotionStatusPeriodGeneratorState()

    def _add_vehicles_with_statuses_and_get_events(
            self, input_data: Iterable[PeriodUpdateInfo], *, system_state: ProcessingSystemState
    ) -> MotionStatusPeriodBoundaryIntermediaryEventsContainer:
        stationary_starts: List[StationaryStartEvent] = []
        moving_starts: List[MovingStartEvent] = []

        for item in input_data: # type: PeriodUpdateInfo
            vehicle_id: str = item.vehicle_id
            motion_status: MotionStatus = item.motion_status
            new_period_id: str = generate_period_uuid()
            # update state
            self._state.add_vehicle_and_start_period(vehicle_id=vehicle_id,
                                                     motion_status=motion_status,
                                                     period_id=new_period_id)
            event_init_args: EventInitArguments = EventInitArguments(camera_id=self._camera_id,
                                                                     event_id=generate_event_uuid(),
                                                                     period_id=new_period_id,
                                                                     vehicle_id=vehicle_id,
                                                                     event_ts=system_state.cur_frame_ts,
                                                                     truncated=system_state.is_first_frame)
            # build event
            match motion_status:
                case MotionStatus.UNDEFINED:
                    # not creating events for undefined (stationary and moving only)
                    pass
                case MotionStatus.STATIONARY:
                    stationary_starts.append(_create_stationary_start_event(event_init_args))
                case MotionStatus.MOVING:
                    moving_starts.append(_create_moving_start_event(event_init_args))
                case _:
                    raise RuntimeError(f"Unknown motion status: {motion_status}")
        container: MotionStatusPeriodBoundaryIntermediaryEventsContainer = (
            MotionStatusPeriodBoundaryIntermediaryEventsContainer(
                start=MotionStatusPeriodStartIntermediaryEventsContainer(stationary=stationary_starts,
                                                                         moving=moving_starts),
                end=MotionStatusPeriodEndIntermediaryEventsContainer.create_empty_container()
            )
        )
        return container

    def _update_vehicles_with_new_statuses_and_get_events(
            self, input_data: Iterable[PeriodUpdateInfo], *, system_state: ProcessingSystemState
    ) -> MotionStatusPeriodBoundaryIntermediaryEventsContainer:
        stationary_ends: List[StationaryEndEvent] = []
        moving_ends: List[MovingEndEvent] = []
        stationary_starts: List[StationaryStartEvent] = []
        moving_starts: List[MovingStartEvent] = []

        for item in input_data: # type: PeriodUpdateInfo

            vehicle_id: str = item.vehicle_id

            # data for the period to begin
            new_motion_status: MotionStatus = item.motion_status
            new_period_id: str = generate_period_uuid()

            # get the old status before removing the vehicle
            old_motion_status: MotionStatus = self._state.get_motion_status_for_vehicle(vehicle_id)
            # update state and get the period id for the period to end
            old_period_id: str = self._state.change_motion_status_and_get_completed_period_id(
                vehicle_id=vehicle_id, new_motion_status=new_motion_status, new_period_id=new_period_id
            )

            end_event_init_args: EventInitArguments = EventInitArguments(camera_id=self._camera_id,
                                                                         event_id=generate_event_uuid(),
                                                                         period_id=old_period_id,
                                                                         vehicle_id=vehicle_id,
                                                                         event_ts=system_state.cur_frame_ts,
                                                                         truncated=system_state.is_first_frame)
            start_event_init_args: EventInitArguments = EventInitArguments(camera_id=self._camera_id,
                                                                           event_id=generate_event_uuid(),
                                                                           period_id=new_period_id,
                                                                           vehicle_id=vehicle_id,
                                                                           event_ts=system_state.cur_frame_ts,
                                                                           truncated=system_state.is_first_frame)

            # generate old period end event
            match old_motion_status:
                case MotionStatus.UNDEFINED:
                    # not creating events for undefined (stationary and moving only)
                    pass
                case MotionStatus.STATIONARY:
                    stationary_ends.append(_create_stationary_end_event(end_event_init_args))
                case MotionStatus.MOVING:
                    moving_ends.append(_create_moving_end_event(end_event_init_args))
                case _:
                    raise RuntimeError(f"Unknown motion status: {old_motion_status}")

            # generate new period start event
            match new_motion_status:
                case MotionStatus.UNDEFINED:
                    # not creating events for undefined (stationary and moving only)
                    pass
                case MotionStatus.STATIONARY:
                    stationary_starts.append(_create_stationary_start_event(start_event_init_args))
                case MotionStatus.MOVING:
                    moving_starts.append(_create_moving_start_event(start_event_init_args))
                case _:
                    raise RuntimeError(f"Unknown motion status: {new_motion_status}")

        # build the container
        container: MotionStatusPeriodBoundaryIntermediaryEventsContainer = (
            MotionStatusPeriodBoundaryIntermediaryEventsContainer(
                start=MotionStatusPeriodStartIntermediaryEventsContainer(stationary=stationary_starts,
                                                                         moving=moving_starts),
                end=MotionStatusPeriodEndIntermediaryEventsContainer(stationary=stationary_ends,
                                                                     moving=moving_ends)
            )
        )
        return container

    def _remove_vehicles_and_get_events(
            self, dead_vehicle_ids: Iterable[str], *, event_ts: datetime, truncate: bool
    ) -> MotionStatusPeriodBoundaryIntermediaryEventsContainer:
        stationary_ends: List[StationaryEndEvent] = []
        moving_ends: List[MovingEndEvent] = []

        for vehicle_id in dead_vehicle_ids: # type: str
            # get the vehicle's last motion status
            last_motion_status: MotionStatus = self._state.get_motion_status_for_vehicle(vehicle_id)
            # remove vehicle from state, get period id
            period_id: str = self._state.remove_vehicle_and_get_completed_period_id(vehicle_id)

            event_init_args: EventInitArguments = EventInitArguments(camera_id=self._camera_id,
                                                                     event_id=generate_event_uuid(),
                                                                     period_id=period_id,
                                                                     vehicle_id=vehicle_id,
                                                                     event_ts=event_ts,
                                                                     truncated=truncate)

            # generate period end event
            match last_motion_status:
                case MotionStatus.UNDEFINED:
                    # not creating events for undefined (stationary and moving only)
                    pass
                case MotionStatus.STATIONARY:
                    stationary_ends.append(_create_stationary_end_event(event_init_args))
                case MotionStatus.MOVING:
                    moving_ends.append(_create_moving_end_event(event_init_args))
                case _:
                    raise RuntimeError(f"Unknown motion status: {last_motion_status}")
        # build the container
        container: MotionStatusPeriodBoundaryIntermediaryEventsContainer = (
            MotionStatusPeriodBoundaryIntermediaryEventsContainer(
                start=MotionStatusPeriodStartIntermediaryEventsContainer.create_empty_container(),
                end=MotionStatusPeriodEndIntermediaryEventsContainer(stationary=stationary_ends,
                                                                     moving=moving_ends)
            )
        )
        return container

    @override
    def _update_and_get_events(
            self, input_data: MotionStatusPeriodGeneratorInput, *, system_state: ProcessingSystemState
    ) -> GlobalMotionStatusPeriodBoundaryEventsContainer:
        # Defined actions:
        # (1) Add a vehicle and start a period for it.
        # (2) Change the motion status for an existing vehicle, starting a new period.
        # (3) Remove the vehicle (ending the period).

        prev_vehicle_ids: Set[str] = self._state.get_vehicle_ids()
        dead_vehicle_ids: Set[str] = {event.vehicle_id for event in input_data.lifetime_events.end}

        # gather information to pass to event creators:
        # (1) for new vehicles
        new_vehicles_infos: List[PeriodUpdateInfo] = []
        # (2) for vehicles whose confirmed motion status has changed
        changed_vehicles_infos: List[PeriodUpdateInfo] = []

        # go through all current confirmed motion statuses
        for event in input_data.confirmed_motion_status_updates.updates: # type: ConfirmedMotionStatusUpdate
            cur_status: MotionStatus = event.motion_status
            vehicle_id: str = event.vehicle_id
            update_info: PeriodUpdateInfo = PeriodUpdateInfo(vehicle_id=vehicle_id,
                                                             motion_status=cur_status)
            if vehicle_id not in prev_vehicle_ids:
                # new vehicle --> to add
                new_vehicles_infos.append(update_info)
            else:
                # existing vehicle ...
                prev_status: MotionStatus = self._state.get_motion_status_for_vehicle(vehicle_id)
                if cur_status is not prev_status:
                    # ... and the status has changed --> to update
                    changed_vehicles_infos.append(update_info)

        # for each group, update state and get events
        new_vehicles_events: MotionStatusPeriodBoundaryIntermediaryEventsContainer = (
            self._add_vehicles_with_statuses_and_get_events(new_vehicles_infos,
                                                            system_state=system_state)
        )
        changed_vehicles_events: MotionStatusPeriodBoundaryIntermediaryEventsContainer = (
            self._update_vehicles_with_new_statuses_and_get_events(changed_vehicles_infos,
                                                                   system_state=system_state)
        )
        dead_vehicles_events: MotionStatusPeriodBoundaryIntermediaryEventsContainer = (
            self._remove_vehicles_and_get_events(dead_vehicle_ids,
                                                 event_ts=system_state.cur_frame_ts,
                                                 truncate=system_state.is_first_frame)
        )
        # concatenate events
        all_events_intermediary_container: MotionStatusPeriodBoundaryIntermediaryEventsContainer = (
            MotionStatusPeriodBoundaryIntermediaryEventsContainer.concatenate(
                (new_vehicles_events, changed_vehicles_events, dead_vehicles_events)
            )
        )
        # convert from intermediary to final container
        all_events: GlobalMotionStatusPeriodBoundaryEventsContainer = all_events_intermediary_container.to_final_container()
        return all_events

    @override
    def _get_events_for_end_of_processing(
            self, *, event_ts: datetime, truncate_events: bool
    ) -> GlobalMotionStatusPeriodBoundaryEventsContainer:
        dead_vehicle_ids: Set[str] = self._state.get_vehicle_ids()
        dead_vehicles_events: MotionStatusPeriodBoundaryIntermediaryEventsContainer = (
            self._remove_vehicles_and_get_events(dead_vehicle_ids,
                                                 event_ts=event_ts,
                                                 truncate=truncate_events)
        )
        final_container: GlobalMotionStatusPeriodBoundaryEventsContainer = dead_vehicles_events.to_final_container()
        return final_container