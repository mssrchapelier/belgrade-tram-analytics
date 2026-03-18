from dataclasses import dataclass, field
from datetime import datetime
from typing import Set, Dict, override, Type, Collection, NamedTuple, List, Self, Sequence, Final

from common.utils.misc_utils import concatenate_sequences
from tram_analytics.v1.models.common_types import MotionStatus
from tram_analytics.v1.models.components.scene_state.events.base import generate_event_uuid, generate_period_uuid
from tram_analytics.v1.models.components.scene_state.events.subprocessors.motion_status.motion_global.updates.confirmed import (
    ConfirmedMotionStatusUpdate
)
from tram_analytics.v1.models.components.scene_state.events.subprocessors.motion_status.motion_in_zone.periods.confirmed import (
    InZoneStationaryStartEvent, InZoneMovingStartEvent, InZoneStationaryEndEvent, InZoneMovingEndEvent
)
from tram_analytics.v1.models.components.scene_state.events.subprocessors.zone_transit import ZoneEntranceEvent, \
    ZoneExitEvent
from tram_analytics.v1.pipeline.components.scene_state.events.subprocessors.base.base_generator import (
    BaseIntermediaryEventGenerator, BaseGeneratorState, ProcessingSystemState
)
from tram_analytics.v1.pipeline.components.scene_state.events.subprocessors.base.base_intermediary_container import \
    BaseIntermediaryEventsContainer
from tram_analytics.v1.pipeline.components.scene_state.events.subprocessors.motion_status.motion_in_zone.periods.confirmed.intermediary_containers import (
    InZoneMotionStatusPeriodBoundaryEventsIntermediaryContainer
)


# --- helper containers for inputs ---

@dataclass(slots=True, kw_only=True)
class ZoneTransitEventsIntermediaryContainer(BaseIntermediaryEventsContainer):
    start: List[ZoneEntranceEvent] = field(default_factory=list)
    end: List[ZoneExitEvent] = field(default_factory=list)

    @override
    @classmethod
    def create_empty_container(cls) -> Self:
        return cls()

    @override
    @classmethod
    def concatenate(cls, containers: Sequence[Self]) -> Self:
        all_entrances: List[ZoneEntranceEvent] = concatenate_sequences(
            tuple(c.start for c in containers)
        )
        all_exits: List[ZoneExitEvent] = concatenate_sequences(
            tuple(c.end for c in containers)
        )
        return cls(start=all_entrances, end=all_exits)

# NOTE: Separated input containers for alive vs dead vehicles to facilitate static type checking
#       as opposed to runtime checks on the internal consistency of the containers.

class InputEventsForDeadVehicle(NamedTuple):
    zone_exit_events: List[ZoneExitEvent]

class InputEventsForAliveVehicle(NamedTuple):
    # all events present for a single vehicle in the update
    cur_motion_status_update: ConfirmedMotionStatusUpdate
    zone_occupancy_events: ZoneTransitEventsIntermediaryContainer


# --- helpers ---

# event creation helpers (to avoid verbosity and boilerplate in the generator class)

class EventInitArguments(NamedTuple):
    camera_id: str
    event_id: str
    period_id: str
    vehicle_id: str
    zone_id: str
    event_ts: datetime
    truncated: bool

def _create_stationary_start_event(init_args: EventInitArguments) -> InZoneStationaryStartEvent:
    return InZoneStationaryStartEvent(camera_id=init_args.camera_id,
                                      event_id=init_args.event_id,
                                      period_id=init_args.period_id,
                                      vehicle_id=init_args.vehicle_id,
                                      zone_id=init_args.zone_id,
                                      event_ts=init_args.event_ts,
                                      truncated=init_args.truncated)

def _create_stationary_end_event(init_args: EventInitArguments) -> InZoneStationaryEndEvent:
    return InZoneStationaryEndEvent(camera_id=init_args.camera_id,
                                    event_id=init_args.event_id,
                                    period_id=init_args.period_id,
                                    vehicle_id=init_args.vehicle_id,
                                    zone_id=init_args.zone_id,
                                    event_ts=init_args.event_ts,
                                    truncated=init_args.truncated)

def _create_moving_start_event(init_args: EventInitArguments) -> InZoneMovingStartEvent:
    return InZoneMovingStartEvent(camera_id=init_args.camera_id,
                                  event_id=init_args.event_id,
                                  period_id=init_args.period_id,
                                  vehicle_id=init_args.vehicle_id,
                                  zone_id=init_args.zone_id,
                                  event_ts=init_args.event_ts,
                                  truncated=init_args.truncated)

def _create_moving_end_event(init_args: EventInitArguments) -> InZoneMovingEndEvent:
    return InZoneMovingEndEvent(camera_id=init_args.camera_id,
                                event_id=init_args.event_id,
                                period_id=init_args.period_id,
                                vehicle_id=init_args.vehicle_id,
                                zone_id=init_args.zone_id,
                                event_ts=init_args.event_ts,
                                truncated=init_args.truncated)

# --- updater state ---

class VehicleStatusUpdaterState(BaseGeneratorState):

    def __init__(self) -> None:
        super().__init__()
        self._period_id: str | None = None
        self._zone_id_to_period_id: Dict[str, str] = dict()

    @override
    def _clear_own_state(self) -> None:
        self._period_id = None
        self._zone_id_to_period_id.clear()

    def get_zone_ids(self) -> Set[str]:
        return set(self._zone_id_to_period_id.keys())

    def add_zone(self, *, zone_id: str, period_id: str) -> None:
        if zone_id in self._zone_id_to_period_id:
            raise ValueError(f"Can't add zone {zone_id}: zone already registered")
        self._zone_id_to_period_id[zone_id] = period_id

    def remove_zone_and_get_period_id(self, zone_id: str) -> str:
        if zone_id not in self._zone_id_to_period_id:
            raise ValueError(f"Can't remove zone {zone_id}: zone not registered")
        return self._zone_id_to_period_id.pop(zone_id)

    def get_period_id_for_zone(self, zone_id: str) -> str:
        if zone_id not in self._zone_id_to_period_id:
            raise ValueError(f"Can't get period ID for zone {zone_id}: zone not registered")
        return self._zone_id_to_period_id[zone_id]

    def update_zone_with_new_period_id(self, *, zone_id: str, period_id: str) -> None:
        if zone_id not in self._zone_id_to_period_id:
            raise ValueError(f"Can't update zone {zone_id}: zone not registered")
        self._zone_id_to_period_id[zone_id] = period_id


# --- updater ---

class VehicleStatusUpdater(
    BaseIntermediaryEventGenerator[
        InputEventsForAliveVehicle, InZoneMotionStatusPeriodBoundaryEventsIntermediaryContainer,
        VehicleStatusUpdaterState
    ]
):

    UNDEFINED_TO_DEFINED_TRANSITION_ERROR_MSG: str = (
        "Inconsistent state: encountered a change in the motion status from defined to undefined"
    )
    UNSUPPORTED_STATUS_MSG: str = "Unsupported motion status: "

    def __init__(self, *, camera_id: str, vehicle_id: str, motion_status: MotionStatus) -> None:
        super().__init__(camera_id)
        self._vehicle_id: Final[str] = vehicle_id
        self._motion_status: MotionStatus = motion_status

    @override
    @classmethod
    def _get_new_state(cls) -> VehicleStatusUpdaterState:
        return VehicleStatusUpdaterState()

    @override
    @classmethod
    def _get_container_class(cls) -> Type[InZoneMotionStatusPeriodBoundaryEventsIntermediaryContainer]:
        return InZoneMotionStatusPeriodBoundaryEventsIntermediaryContainer

    def _check_vehicle_ids(self, input_for_vehicle: InputEventsForAliveVehicle) -> None:
        zone_events: ZoneTransitEventsIntermediaryContainer = input_for_vehicle.zone_occupancy_events
        vehicle_ids_present: Set[str] = {
            input_for_vehicle.cur_motion_status_update.vehicle_id,
            *{e.vehicle_id for e in (*zone_events.start, *zone_events.end)}
        }
        # all ids must be the same
        if len(vehicle_ids_present) not in (0, 1):
            raise ValueError("All input events must have the same vehicle_id, found more than one: {}".format(
                ", ".join(vehicle_ids_present)
            ))
        # if not empty, the id present must be the same as this instance's stored vehicle id
        id_in_set: str | None = next(iter(vehicle_ids_present)) if len(vehicle_ids_present) > 0 else None
        if id_in_set is not None and id_in_set != self._vehicle_id:
            raise ValueError("The input events have a different vehicle_id from that stored in this instance: "
                             f"expected {self._vehicle_id}, got {id_in_set}")

    @staticmethod
    def _check_zone_ids_unique(input_for_vehicle: InputEventsForAliveVehicle) -> None:
        zone_events: ZoneTransitEventsIntermediaryContainer = input_for_vehicle.zone_occupancy_events
        total_events: int = len(zone_events.start) + len(zone_events.end)
        zone_ids: Set[str] = {e.zone_id for e in (*zone_events.start, *zone_events.end)}
        total_zone_ids: int = len(zone_ids)
        if total_zone_ids != total_events:
            raise ValueError("All zone events must have unique zone IDs, but encountered duplicates")

    def _check_input(self, input_for_vehicle: InputEventsForAliveVehicle) -> None:
        self._check_vehicle_ids(input_for_vehicle)
        self._check_zone_ids_unique(input_for_vehicle)

    def _compute_cur_zone_ids(self, zone_events: ZoneTransitEventsIntermediaryContainer) -> Set[str]:
        """
        Compute the current zone IDs for this vehicle, taking the set for the previous frame as the basis
        and adding / removing zones based on zone occupancy events for this vehicle.
        """
        zone_ids: Set[str] = self._state.get_zone_ids()
        for entrance_event in zone_events.start: # type: ZoneEntranceEvent
            if entrance_event.zone_id in zone_ids:
                raise ValueError(f"Received a zone entrance event, but this vehicle is registered "
                                 f"as already being in this zone: {entrance_event.zone_id}")
            zone_ids.add(entrance_event.zone_id)
        for exit_event in zone_events.end: # type: ZoneExitEvent
            if exit_event.zone_id not in zone_ids:
                raise ValueError(f"Received a zone exit event, but this vehicle is not registered "
                                 f"as being in this zone: {exit_event.zone_id}")
            zone_ids.remove(exit_event.zone_id)
        return zone_ids

    def _update_and_get_events_for_zone_changes(
            self, *, zone_transit_events: ZoneTransitEventsIntermediaryContainer,
            cur_motion_status: MotionStatus,
            system_state: ProcessingSystemState
    ) -> InZoneMotionStatusPeriodBoundaryEventsIntermediaryContainer:

        cur_frame_ts: datetime = system_state.cur_frame_ts
        is_first_frame: bool = system_state.is_first_frame

        container: InZoneMotionStatusPeriodBoundaryEventsIntermediaryContainer = (
            InZoneMotionStatusPeriodBoundaryEventsIntermediaryContainer()
        )
        # create events for zone entrances ...
        for entrance_event in zone_transit_events.start: # type: ZoneEntranceEvent
            # ... based on the CURRENT motion status
            new_zone_id: str = entrance_event.zone_id
            new_period_id: str = generate_period_uuid()
            self._state.add_zone(zone_id=new_zone_id, period_id=new_period_id)
            start_event_init_args: EventInitArguments = EventInitArguments(camera_id=self._camera_id,
                                                                           event_id=generate_event_uuid(),
                                                                           period_id=new_period_id,
                                                                           vehicle_id=self._vehicle_id,
                                                                           zone_id=new_zone_id,
                                                                           event_ts=cur_frame_ts,
                                                                           truncated=is_first_frame)
            match cur_motion_status:
                case MotionStatus.STATIONARY:
                    container.start.stationary.append(_create_stationary_start_event(start_event_init_args))
                case MotionStatus.MOVING:
                    container.start.moving.append(_create_moving_start_event(start_event_init_args))
                case MotionStatus.UNDEFINED:
                    pass
                case _:
                    raise ValueError(self.UNSUPPORTED_STATUS_MSG + cur_motion_status)
        # create events for zone exits ...
        for exit_event in zone_transit_events.end: # type: ZoneExitEvent
            # ... based on the PREVIOUS motion status
            prev_status: MotionStatus | None = self._motion_status
            dead_zone_id: str = exit_event.zone_id
            ended_period_id: str = self._state.remove_zone_and_get_period_id(dead_zone_id)
            end_event_init_args: EventInitArguments = EventInitArguments(camera_id=self._camera_id,
                                                                         event_id=generate_event_uuid(),
                                                                         period_id=ended_period_id,
                                                                         vehicle_id=self._vehicle_id,
                                                                         zone_id=dead_zone_id,
                                                                         event_ts=cur_frame_ts,
                                                                         truncated=is_first_frame)
            match prev_status:
                # NOTE: do not truncate end events here; are only truncated when ending processing
                case MotionStatus.STATIONARY:
                    container.end.stationary.append(_create_stationary_end_event(end_event_init_args))
                case MotionStatus.MOVING:
                    container.end.moving.append(_create_moving_end_event(end_event_init_args))
                case MotionStatus.UNDEFINED:
                    pass
                case _:
                    raise ValueError(self.UNSUPPORTED_STATUS_MSG + self._motion_status)
        return container

    def _update_and_get_events_for_continued_zones(
            self, *, continued_zone_ids: Collection[str],
            cur_motion_status: MotionStatus,
            system_state: ProcessingSystemState
    ) -> InZoneMotionStatusPeriodBoundaryEventsIntermediaryContainer:
        cur_frame_ts: datetime = system_state.cur_frame_ts
        is_first_frame: bool = system_state.is_first_frame

        if is_first_frame and len(continued_zone_ids) > 0:
            # logically impossible: no zones could have been registered (and thus unchanged)
            # if this is the first frame
            raise ValueError("Incompatible input: is_first_frame is set to True, "
                             "but continued_zone_ids is not empty")

        prev_status: MotionStatus = self._motion_status
        cur_status: MotionStatus = cur_motion_status
        container: InZoneMotionStatusPeriodBoundaryEventsIntermediaryContainer = (
            InZoneMotionStatusPeriodBoundaryEventsIntermediaryContainer()
        )

        # NOTE: null for cur_status can only be received in the case of the vehicle's lifetime end --
        # produce end events for all zones in this case appropriately

        for zone_id in continued_zone_ids: # type: str

            # If there is a change in status,
            # (1) the stored status will be changed and (2) a new period id will be stored for the zone.
            # Events will be produced only when the old/new period is not "undefined";
            # accordingly, at most two events:
            # (1) an end event (if the previous status was not undefined);
            # (2) and a start event (if the current status is not undefined).

            prev_period_id: str = self._state.get_period_id_for_zone(zone_id)
            new_period_id: str = generate_period_uuid()

            if cur_status is not prev_status:
                # start a new period for the zone
                self._state.update_zone_with_new_period_id(zone_id=zone_id, period_id=new_period_id)

            start_event_init_args: EventInitArguments = EventInitArguments(camera_id=self._camera_id,
                                                                           event_id=generate_event_uuid(),
                                                                           period_id=new_period_id,
                                                                           vehicle_id=self._vehicle_id,
                                                                           zone_id=zone_id,
                                                                           event_ts=cur_frame_ts,
                                                                           truncated=is_first_frame)
            end_event_init_args: EventInitArguments = EventInitArguments(camera_id=self._camera_id,
                                                                         event_id=generate_event_uuid(),
                                                                         period_id=prev_period_id,
                                                                         vehicle_id=self._vehicle_id,
                                                                         zone_id=zone_id,
                                                                         event_ts=cur_frame_ts,
                                                                         truncated=is_first_frame)

            if prev_status is MotionStatus.UNDEFINED:
                match cur_status:
                    case MotionStatus.UNDEFINED:
                        pass
                    case MotionStatus.STATIONARY:
                        container.start.stationary.append(_create_stationary_start_event(start_event_init_args))
                    case MotionStatus.MOVING:
                        container.start.moving.append(_create_moving_start_event(start_event_init_args))
                    case _:
                        raise ValueError(self.UNSUPPORTED_STATUS_MSG + cur_motion_status)
            # NOTE re previous status and end events:
            #   do not truncate end events here; are only truncated when ending processing
            elif prev_status is MotionStatus.MOVING:
                match cur_status:
                    case MotionStatus.UNDEFINED:
                        # defined -> undefined: by design, should never happen
                        raise RuntimeError(self.UNDEFINED_TO_DEFINED_TRANSITION_ERROR_MSG)
                    case MotionStatus.MOVING:
                        pass
                    case MotionStatus.STATIONARY:
                        container.end.moving.append(_create_moving_end_event(end_event_init_args))
                        container.start.stationary.append(_create_stationary_start_event(start_event_init_args))
                    case _:
                        raise ValueError(self.UNSUPPORTED_STATUS_MSG + cur_motion_status)

            elif prev_status is MotionStatus.STATIONARY:
                match cur_status:
                    case MotionStatus.UNDEFINED:
                        # defined -> undefined: by design, should never happen
                        raise RuntimeError(self.UNDEFINED_TO_DEFINED_TRANSITION_ERROR_MSG)
                    case MotionStatus.STATIONARY:
                        pass
                    case MotionStatus.MOVING:
                        container.end.stationary.append(_create_stationary_end_event(end_event_init_args))
                        container.start.moving.append(_create_moving_start_event(start_event_init_args))
                    case _:
                        raise ValueError(self.UNSUPPORTED_STATUS_MSG + cur_motion_status)

            else:
                raise ValueError(self.UNSUPPORTED_STATUS_MSG + cur_motion_status)

        return container

    @override
    def _update_and_get_events(
            self, input_for_vehicle: InputEventsForAliveVehicle,
            *, system_state: ProcessingSystemState
    ) -> InZoneMotionStatusPeriodBoundaryEventsIntermediaryContainer:
        self._check_input(input_for_vehicle)
        cur_motion_status: MotionStatus = (
            input_for_vehicle.cur_motion_status_update.motion_status
        )

        if (
                cur_motion_status is MotionStatus.UNDEFINED
                and self._motion_status is not MotionStatus.UNDEFINED
        ):
            # defined -> undefined: by design, should never happen
            raise RuntimeError("Inconsistent state: encountered a change "
                               "in the confirmed motion status from defined to undefined")

        prev_zone_ids: Set[str] = self._state.get_zone_ids()
        # calculate all zones to which the vehicle is currently assigned
        cur_zone_ids: Set[str] = self._compute_cur_zone_ids(input_for_vehicle.zone_occupancy_events)
        # select zones to which the vehicle was already assigned
        continued_zone_ids: Set[str] = set.intersection(prev_zone_ids, cur_zone_ids)

        # produce in-zone motion status change events for zone entrances/exits
        # (depending on the vehicle's previous and current confirmed motion status)
        dest_events_for_zone_changes: InZoneMotionStatusPeriodBoundaryEventsIntermediaryContainer = (
            self._update_and_get_events_for_zone_changes(
                zone_transit_events=input_for_vehicle.zone_occupancy_events,
                cur_motion_status=cur_motion_status,
                system_state=system_state
            )
        )
        # produce in-zone motion status change events for unchanged assigned zones
        # (depending on the vehicle's previous and current confirmed motion status)
        dest_events_for_continued_zones: InZoneMotionStatusPeriodBoundaryEventsIntermediaryContainer = (
            self._update_and_get_events_for_continued_zones(
                continued_zone_ids=continued_zone_ids, cur_motion_status=cur_motion_status,
                system_state=system_state
            )
        )

        # build the output container with all events for this vehicle
        container: InZoneMotionStatusPeriodBoundaryEventsIntermediaryContainer = (
            InZoneMotionStatusPeriodBoundaryEventsIntermediaryContainer.concatenate(
                (dest_events_for_zone_changes, dest_events_for_continued_zones)
            )
        )

        # update this vehicle's state
        self._motion_status = cur_motion_status
        self._zone_ids = cur_zone_ids

        return container

    @override
    def _get_events_for_end_of_processing(
            self, *, event_ts: datetime, truncate_events: bool
    ) -> InZoneMotionStatusPeriodBoundaryEventsIntermediaryContainer:
        # produce exit events for all zones according to the vehicle's status
        prev_status: MotionStatus = self._motion_status
        container: InZoneMotionStatusPeriodBoundaryEventsIntermediaryContainer = (
            InZoneMotionStatusPeriodBoundaryEventsIntermediaryContainer()
        )
        for zone_id in self._zone_ids: # type: str
            ended_period_id: str = self._state.remove_zone_and_get_period_id(zone_id)
            end_event_init_args: EventInitArguments = EventInitArguments(camera_id=self._camera_id,
                                                                         event_id=generate_event_uuid(),
                                                                         period_id=ended_period_id,
                                                                         vehicle_id=self._vehicle_id,
                                                                         zone_id=zone_id,
                                                                         event_ts=event_ts,
                                                                         truncated=truncate_events)
            match prev_status:
                case MotionStatus.UNDEFINED:
                    pass
                case MotionStatus.STATIONARY:
                    container.end.stationary.append(_create_stationary_end_event(end_event_init_args))
                case MotionStatus.MOVING:
                    container.end.moving.append(_create_moving_end_event(end_event_init_args))
                case _:
                    raise ValueError(self.UNSUPPORTED_STATUS_MSG + prev_status)
        return container
