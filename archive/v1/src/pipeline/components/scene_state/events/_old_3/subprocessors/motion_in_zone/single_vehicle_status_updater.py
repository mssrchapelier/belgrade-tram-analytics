from datetime import datetime
from typing import Set, override, Type, Collection

from tram_analytics.v1.models.components.scene_state.events.base import generate_event_uuid
from archive.v1.src.pipeline.components.scene_state.events._old_3.subprocessors.base.base_generator import BaseIntermediaryEventGenerator, ProcessingSystemState
from tram_analytics.v1.models.components.scene_state.events.subprocessors.zone_transit import ZoneEntranceEvent, ZoneExitEvent
from archive.v1.src.pipeline.components.scene_state.events._old_3.subprocessors.motion_in_zone.events import InZoneStationaryStartEvent, InZoneMovingStartEvent, \
    InZoneStationaryEndEvent, InZoneMovingEndEvent
from archive.v1.src.pipeline.components.scene_state.events._old_3.subprocessors.motion_in_zone.intermediary_containers import InputEventsForAliveVehicle, \
    IntermediaryOutputEventsContainer, IntermediaryZoneEventsContainer
from tram_analytics.v1.models.common_types import MotionStatus


class VehicleStatusUpdater(
    BaseIntermediaryEventGenerator[InputEventsForAliveVehicle, IntermediaryOutputEventsContainer]
):

    UNDEFINED_TO_DEFINED_TRANSITION_ERROR_MSG: str = (
        "Inconsistent state: encountered a change "
        "in the confirmed motion status from defined to undefined"
    )
    UNSUPPORTED_STATUS_MSG: str = "Unsupported confirmed motion status: "

    def __init__(self, vehicle_id: str) -> None:
        super().__init__()
        self._vehicle_id: str = vehicle_id
        self._prev_confirmed_motion_status: MotionStatus = MotionStatus.UNDEFINED
        self._prev_zone_ids: Set[str] = set()

    @override
    @classmethod
    def _get_container_class(cls) -> Type[IntermediaryOutputEventsContainer]:
        return IntermediaryOutputEventsContainer

    def _check_vehicle_ids(self, input_for_vehicle: InputEventsForAliveVehicle) -> None:
        zone_events: IntermediaryZoneEventsContainer = input_for_vehicle.zone_occupancy_events
        vehicle_ids_present: Set[str] = {
            input_for_vehicle.current_confirmed_motion_status_update_event.vehicle_id,
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
        zone_events: IntermediaryZoneEventsContainer = input_for_vehicle.zone_occupancy_events
        total_events: int = len(zone_events.start) + len(zone_events.end)
        zone_ids: Set[str] = {e.zone_id for e in (*zone_events.start, *zone_events.end)}
        total_zone_ids: int = len(zone_ids)
        if total_zone_ids != total_events:
            raise ValueError("All zone events must have unique zone IDs, but encountered duplicates")

    def _check_input(self, input_for_vehicle: InputEventsForAliveVehicle) -> None:
        self._check_vehicle_ids(input_for_vehicle)
        self._check_zone_ids_unique(input_for_vehicle)

    def _compute_cur_zone_ids(self, zone_events: IntermediaryZoneEventsContainer) -> Set[str]:
        """
        Compute the current zone IDs for this vehicle, taking the set for the previous frame as the basis
        and adding / removing zones based on zone occupancy events for this vehicle.
        """
        zone_ids: Set[str] = self._prev_zone_ids.copy()
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

    def _produce_events_for_zone_changes(
            self, *, zone_events: IntermediaryZoneEventsContainer,
            cur_motion_status: MotionStatus,
            system_state: ProcessingSystemState
    ) -> IntermediaryOutputEventsContainer:
        cur_frame_ts: datetime = system_state.cur_frame_ts
        is_first_frame: bool = system_state.is_first_frame

        container: IntermediaryOutputEventsContainer = IntermediaryOutputEventsContainer()
        # create events for zone entrances ...
        for entrance_event in zone_events.start: # type: ZoneEntranceEvent
            # ... based on the CURRENT motion status
            match cur_motion_status:
                case MotionStatus.STATIONARY:
                    container.start.stationary.append(
                        InZoneStationaryStartEvent(event_id=generate_event_uuid(),
                                                   vehicle_id=entrance_event.vehicle_id,
                                                   zone_id=entrance_event.zone_id,
                                                   event_ts=cur_frame_ts,
                                                   truncated=is_first_frame
                                                   ))
                case MotionStatus.MOVING:
                    container.start.moving.append(
                        InZoneMovingStartEvent(event_id=generate_event_uuid(),
                                               vehicle_id=entrance_event.vehicle_id,
                                               zone_id=entrance_event.zone_id,
                                               event_ts=cur_frame_ts,
                                               truncated=is_first_frame)
                    )
                case MotionStatus.UNDEFINED:
                    pass
                # case None:
                #     raise ValueError("Got no motion status in input, which is only possible "
                #                      "for the vehicle's lifetime end, but input contains zone entrance events")
                case _:
                    raise ValueError(self.UNSUPPORTED_STATUS_MSG + cur_motion_status)
        # create events for zone exits ...
        for exit_event in zone_events.end: # type: ZoneExitEvent
            # ... based on the PREVIOUS motion status
            match self._prev_confirmed_motion_status:
                # NOTE: do not truncate end events here; are only truncated when ending processing
                case MotionStatus.STATIONARY:
                    container.end.stationary.append(
                        InZoneStationaryEndEvent(event_id=generate_event_uuid(),
                                                 vehicle_id=exit_event.vehicle_id,
                                                 zone_id=exit_event.zone_id,
                                                 event_ts=cur_frame_ts,
                                                 truncated=False))
                case MotionStatus.MOVING:
                    container.end.moving.append(
                        InZoneMovingEndEvent(event_id=generate_event_uuid(),
                                             vehicle_id=exit_event.vehicle_id,
                                             zone_id=exit_event.zone_id,
                                             event_ts=cur_frame_ts,
                                             truncated=False)
                    )
                case MotionStatus.UNDEFINED:
                    pass
                case _:
                    raise ValueError(self.UNSUPPORTED_STATUS_MSG + self._prev_confirmed_motion_status)
        return container

    def _produce_events_for_unchanged_zones(
            self, *, unchanged_zone_ids: Collection[str],
            cur_motion_status: MotionStatus,
            system_state: ProcessingSystemState
    ) -> IntermediaryOutputEventsContainer:
        cur_frame_ts: datetime = system_state.cur_frame_ts
        is_first_frame: bool = system_state.is_first_frame

        if is_first_frame and len(unchanged_zone_ids) > 0:
            # logically impossible: no zones could have been registered (and thus unchanged)
            # if this is the first frame
            raise ValueError("Incompatible input: is_first_frame is set to True, "
                             "but unchanged_zone_ids is not empty")

        prev_status: MotionStatus = self._prev_confirmed_motion_status
        cur_status: MotionStatus = cur_motion_status
        container: IntermediaryOutputEventsContainer = IntermediaryOutputEventsContainer()

        # NOTE: null for cur_status can only be received in the case of the vehicle's lifetime end --
        # produce end events for all zones in this case appropriately

        for zone_id in unchanged_zone_ids: # type: str

            if prev_status is MotionStatus.UNDEFINED:
                match cur_status:
                    case MotionStatus.UNDEFINED:
                        pass
                    case MotionStatus.STATIONARY:
                        container.start.stationary.append(
                            InZoneStationaryStartEvent(event_id=generate_event_uuid(),
                                                       vehicle_id=self._vehicle_id,
                                                       zone_id=zone_id,
                                                       event_ts=cur_frame_ts,
                                                       truncated=is_first_frame)
                        )
                    case MotionStatus.MOVING:
                        container.start.moving.append(
                            InZoneMovingStartEvent(event_id=generate_event_uuid(),
                                                   vehicle_id=self._vehicle_id,
                                                   zone_id=zone_id,
                                                   event_ts=cur_frame_ts,
                                                   truncated=is_first_frame)
                        )
                    # case None:
                    #     pass
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
                        container.end.moving.append(InZoneMovingEndEvent(event_id=generate_event_uuid(),
                                                                         vehicle_id=self._vehicle_id,
                                                                         zone_id=zone_id,
                                                                         event_ts=cur_frame_ts,
                                                                         truncated=False))
                        container.start.stationary.append(InZoneStationaryStartEvent(event_id=generate_event_uuid(),
                                                                                     vehicle_id=self._vehicle_id,
                                                                                     zone_id=zone_id,
                                                                                     event_ts=cur_frame_ts,
                                                                                     truncated=is_first_frame))
                    # case None:
                    #     container.end.moving.append(InZoneMovingEndEvent(vehicle_id=self._vehicle_id,
                    #                                              zone_id=zone_id))
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
                        container.end.stationary.append(InZoneStationaryEndEvent(event_id=generate_event_uuid(),
                                                                                 vehicle_id=self._vehicle_id,
                                                                                 zone_id=zone_id,
                                                                                 event_ts=cur_frame_ts,
                                                                                 truncated=False))
                        container.start.moving.append(InZoneMovingStartEvent(event_id=generate_event_uuid(),
                                                                             vehicle_id=self._vehicle_id,
                                                                             zone_id=zone_id,
                                                                             event_ts=cur_frame_ts,
                                                                             truncated=is_first_frame))
                    # case None:
                    #     container.end.stationary.append(InZoneStationaryEndEvent(vehicle_id=self._vehicle_id,
                    #                                                              zone_id=zone_id))
                    case _:
                        raise ValueError(self.UNSUPPORTED_STATUS_MSG + cur_motion_status)

            else:
                raise ValueError(self.UNSUPPORTED_STATUS_MSG + cur_motion_status)

        return container

    @override
    def _update_and_get_events(
            self, input_for_vehicle: InputEventsForAliveVehicle,
            *, system_state: ProcessingSystemState
    ) -> IntermediaryOutputEventsContainer:
        self._check_input(input_for_vehicle)
        cur_confirmed_motion_status: MotionStatus = (
            input_for_vehicle.current_confirmed_motion_status_update_event.confirmed
        )

        if (
                cur_confirmed_motion_status is MotionStatus.UNDEFINED
                and self._prev_confirmed_motion_status is not MotionStatus.UNDEFINED
        ):
            # defined -> undefined: by design, should never happen
            raise RuntimeError("Inconsistent state: encountered a change "
                               "in the confirmed motion status from defined to undefined")

        # calculate all zones to which the vehicle is currently assigned
        cur_zone_ids: Set[str] = self._compute_cur_zone_ids(input_for_vehicle.zone_occupancy_events)
        # select zones to which the vehicle was already assigned
        unchanged_zone_ids: Set[str] = set.intersection(self._prev_zone_ids, cur_zone_ids)

        # produce in-zone motion status change events for zone entrances/exits
        # (depending on the vehicle's previous and current confirmed motion status)
        dest_events_for_zone_changes: IntermediaryOutputEventsContainer = self._produce_events_for_zone_changes(
            zone_events=input_for_vehicle.zone_occupancy_events,
            cur_motion_status=cur_confirmed_motion_status,
            system_state=system_state
        )
        # produce in-zone motion status change events for unchanged assigned zones
        # (depending on the vehicle's previous and current confirmed motion status)
        dest_events_for_unchanged_zones: IntermediaryOutputEventsContainer = self._produce_events_for_unchanged_zones(
            unchanged_zone_ids=unchanged_zone_ids, cur_motion_status=cur_confirmed_motion_status,
            system_state=system_state
        )

        # build the output container with all events for this vehicle
        container: IntermediaryOutputEventsContainer = IntermediaryOutputEventsContainer.concatenate(
            (dest_events_for_zone_changes, dest_events_for_unchanged_zones)
        )

        # update this vehicle's state
        self._prev_confirmed_motion_status = cur_confirmed_motion_status
        self._prev_zone_ids = cur_zone_ids

        return container

    @override
    def _get_events_for_end_of_processing(self,
                                          *, event_ts: datetime,
                                          truncate_events: bool) -> IntermediaryOutputEventsContainer:
        # produce exit events for all zones according to the vehicle's status
        prev_status: MotionStatus = self._prev_confirmed_motion_status
        container: IntermediaryOutputEventsContainer = IntermediaryOutputEventsContainer()
        for zone_id in self._prev_zone_ids: # type: str
            match prev_status:
                case MotionStatus.UNDEFINED:
                    pass
                case MotionStatus.STATIONARY:
                    container.end.stationary.append(
                        InZoneStationaryEndEvent(event_id=generate_event_uuid(),
                                                 vehicle_id=self._vehicle_id,
                                                 zone_id=zone_id,
                                                 event_ts=event_ts,
                                                 truncated=truncate_events)
                    )
                case MotionStatus.MOVING:
                    container.end.moving.append(
                        InZoneMovingEndEvent(event_id=generate_event_uuid(),
                                             vehicle_id=self._vehicle_id,
                                             zone_id=zone_id,
                                             event_ts=event_ts,
                                             truncated=truncate_events)
                    )
                case _:
                    raise ValueError(self.UNSUPPORTED_STATUS_MSG + prev_status)
        return container

    @override
    def _clear_own_state(self) -> None:
        # TODO: possibly also clear vehicle_id and prev_confirmed_motion_status
        # (but need to be settable to None in that case; more runtime state validation)
        self._prev_zone_ids.clear()
