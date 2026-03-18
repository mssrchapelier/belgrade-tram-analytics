from datetime import datetime
from typing import List, Set, Dict, Iterable, NamedTuple, override, Type
from warnings import deprecated

from tram_analytics.v1.models.common_types import MotionStatus
from tram_analytics.v1.models.components.scene_state.events.subprocessors.lifetime import (
    VehicleLifetimeEndEvent, VehiclesLifetimeEventsContainer
)
from tram_analytics.v1.models.components.scene_state.events.subprocessors.motion_status.motion_global.updates.confirmed import (
    ConfirmedMotionStatusUpdate, ConfirmedMotionStatusUpdatesContainer
)
from tram_analytics.v1.models.components.scene_state.events.subprocessors.motion_status.motion_in_zone.periods.confirmed import (
    InZoneMotionStatusStartEventsContainer, InZoneMotionStatusEndEventsContainer,
    InZoneMotionStatusPeriodBoundaryEventsContainer
)
from tram_analytics.v1.models.components.scene_state.events.subprocessors.zone_transit import (
    ZoneEntranceEvent, ZoneExitEvent, ZoneTransitEventsContainer
)
from tram_analytics.v1.pipeline.components.scene_state.events.subprocessors.base.base_generator import (
    BaseFinalEventGenerator, BaseGeneratorState, ProcessingSystemState
)
from tram_analytics.v1.pipeline.components.scene_state.events.subprocessors.motion_status.motion_in_zone.periods.confirmed.intermediary_containers import (
    InZoneMotionStatusPeriodBoundaryEventsIntermediaryContainer
)
from tram_analytics.v1.pipeline.components.scene_state.events.subprocessors.motion_status.motion_in_zone.periods.confirmed.single_vehicle_status_updater import (
    ZoneTransitEventsIntermediaryContainer, InputEventsForAliveVehicle, InputEventsForDeadVehicle,
    VehicleStatusUpdater
)


# --- generator state ---

class VehicleInitArgs(NamedTuple):
    vehicle_id: str
    motion_status: MotionStatus

class InZoneMotionStatusPeriodBoundaryEventGeneratorState(BaseGeneratorState):

    def __init__(self):
        super().__init__()
        # vehicle id -> updater object, for all alive vehicles
        self._vehicle_id_to_updater: Dict[str, VehicleStatusUpdater] = dict()

    @override
    def _clear_own_state(self) -> None:
        self._vehicle_id_to_updater.clear()

    def get_vehicle_ids(self) -> Set[str]:
        return set(self._vehicle_id_to_updater.keys())

    def get_updater_for_vehicle(self, vehicle_id: str) -> VehicleStatusUpdater:
        if not vehicle_id in self._vehicle_id_to_updater:
            raise ValueError(f"Can't get updater for vehicle {vehicle_id}: not registered")
        return self._vehicle_id_to_updater[vehicle_id]

    def add_vehicle_and_assign_updater(self, *, vehicle_id: str, updater: VehicleStatusUpdater) -> None:
        if vehicle_id in self._vehicle_id_to_updater:
            raise ValueError(f"Can't register vehicle {vehicle_id}: already registered")
        self._vehicle_id_to_updater[vehicle_id] = updater

    def remove_vehicle(self, vehicle_id: str) -> None:
        if vehicle_id not in self._vehicle_id_to_updater:
            raise ValueError(f"Can't remove vehicle {vehicle_id}: not registered")
        self._vehicle_id_to_updater.pop(vehicle_id)

# --- generator ---

# helper containers

class InputForAliveVehicleWithVehicleID(NamedTuple):
    vehicle_id: str
    events: InputEventsForAliveVehicle

class InputForDeadVehicleWithVehicleID(NamedTuple):
    vehicle_id: str
    events: InputEventsForDeadVehicle

class InputForVehicles(NamedTuple):
    alive: List[InputForAliveVehicleWithVehicleID]
    dead: List[InputForDeadVehicleWithVehicleID]

# input to the generator
class InZoneMotionStatusPeriodBoundaryEventGeneratorInput(NamedTuple):
    lifetime_events: VehiclesLifetimeEventsContainer
    global_motion_status_updates: ConfirmedMotionStatusUpdatesContainer
    zone_transit_events: ZoneTransitEventsContainer

# generator
class InZoneMotionStatusPeriodBoundaryEventGenerator(
    BaseFinalEventGenerator[InZoneMotionStatusPeriodBoundaryEventGeneratorInput,
                            InZoneMotionStatusPeriodBoundaryEventsContainer,
                            InZoneMotionStatusPeriodBoundaryEventGeneratorState]
):

    @override
    @classmethod
    def _get_container_class(cls) -> Type[InZoneMotionStatusPeriodBoundaryEventsContainer]:
        return InZoneMotionStatusPeriodBoundaryEventsContainer

    @override
    def _get_new_state(self) -> InZoneMotionStatusPeriodBoundaryEventGeneratorState:
        return InZoneMotionStatusPeriodBoundaryEventGeneratorState()

    def _add_updaters_for_new_vehicles(
            self, input_data: InZoneMotionStatusPeriodBoundaryEventGeneratorInput
    ) -> None:
        new_vehicle_ids: Set[str] = {lifetime_start_event.vehicle_id
                                     for lifetime_start_event in input_data.lifetime_events.start}
        for update in input_data.global_motion_status_updates.updates: # type: ConfirmedMotionStatusUpdate
            vehicle_id: str = update.vehicle_id
            motion_status: MotionStatus = update.motion_status
            if vehicle_id in new_vehicle_ids:
                vehicle_updater: VehicleStatusUpdater = VehicleStatusUpdater(camera_id=self._camera_id,
                                                                             vehicle_id=vehicle_id,
                                                                             motion_status=motion_status)
                self._state.add_vehicle_and_assign_updater(vehicle_id=vehicle_id, updater=vehicle_updater)

    @deprecated("Deprecated, see details inside the method")
    def _remove_updaters_for_dead_vehicles(self, lifetime_end_events: Iterable[VehicleLifetimeEndEvent]) -> None:
        # call AFTER any processing of stationary and zone events, to delete the updaters for dead vehicles
        # UPDATE: DEPRECATED - For dead vehicles, updaters are removed immediately after getting final events from them;
        # calling this method will now throw a KeyError accordingly.
        for event in lifetime_end_events:
            vehicle_id: str = event.vehicle_id
            self._state.remove_vehicle(vehicle_id)

    def _update_and_get_events_for_alive_vehicles(
            self, input_events_by_vehicle: List[InputForAliveVehicleWithVehicleID],
            *, system_state: ProcessingSystemState
    ) -> InZoneMotionStatusPeriodBoundaryEventsIntermediaryContainer:
        containers: List[InZoneMotionStatusPeriodBoundaryEventsIntermediaryContainer] = []
        for input_for_vehicle in input_events_by_vehicle:  # type: InputForAliveVehicleWithVehicleID
            # select the updater for this vehicle
            updater: VehicleStatusUpdater = self._state.get_updater_for_vehicle(
                input_for_vehicle.vehicle_id
            )
            vehicle_output: InZoneMotionStatusPeriodBoundaryEventsIntermediaryContainer = updater.update_and_get_events(
                input_for_vehicle.events, system_state=system_state
            )
            containers.append(vehicle_output)
        return InZoneMotionStatusPeriodBoundaryEventsIntermediaryContainer.concatenate(containers)

    @staticmethod
    def _check_for_dead_vehicle_end_output_events_only(
            output_for_vehicle: InZoneMotionStatusPeriodBoundaryEventsIntermediaryContainer
    ) -> None:
        # in the output, end events only
        if not(len(output_for_vehicle.start.stationary) == 0
               and len(output_for_vehicle.start.moving) == 0):
            raise RuntimeError("Inconsistent output for in-zone motion status change events: "
                               "For a dead vehicle, only end events are expected to be produced, "
                               "but got start events in the output")

    @staticmethod
    @deprecated("Deprecated, use _check_for_dead_vehicle_all_output_zone_ids_in_input instead")
    def _check_same_zone_ids_in_input_and_output_for_dead_vehicle(
            *, input_for_vehicle: InputEventsForDeadVehicle,
            output_for_vehicle: InZoneMotionStatusPeriodBoundaryEventsIntermediaryContainer
    ) -> None:
        # Runtime check:
        #   (ABANDONED)
        #   The set of zone IDs in the return must be the same as that in the input events.
        #   If it isn't, this indicates an inconsistent state (likely due to an error in processing logic):
        #   by design, the zone exit events in input must have been generated for the exact set of zones
        #   that were registered in the updater at this step.
        # ABANDONED - Does NOT hold (expectedly indeed) if the vehicle's confirmed motion status was "undefined"
        #   (in that case, no output events will have been generated -- as expected).
        #   Since the vehicles' motion statuses are only meant to be tracked in individual vehicles' updaters,
        #   there is no way to check this condition outside of them; relying on the logic
        #   in their implementation of `_end_processing_and_get_events`.
        zone_ids_in_input: Set[str] = {e.zone_id for e in input_for_vehicle.zone_exit_events}
        zone_ids_in_output: Set[str] = set(output_for_vehicle.iter_over_zone_ids())
        if zone_ids_in_input != zone_ids_in_output:
            raise RuntimeError("Inconsistency between input (zone exit events passed) "
                               "and output (in-zone motion status change events): "
                               "expected to contain the same set of zone IDs, "
                               "but the sets contained in input and output events differ")

    @staticmethod
    def _check_for_dead_vehicle_all_output_zone_ids_in_input(
            *, input_for_vehicle: InputEventsForDeadVehicle,
            output_for_vehicle: InZoneMotionStatusPeriodBoundaryEventsIntermediaryContainer
    ) -> None:
        # Runtime check:
        #   The set of zone IDs in the return must be a strict subset of that in the input events.
        #   If it isn't, then for some of the zones that were still registered in the vehicle's updater,
        #   no zone exit events had been generated, which, by design, shouldn't happen.
        zone_ids_in_input: Set[str] = {e.zone_id for e in input_for_vehicle.zone_exit_events}
        zone_ids_in_output: Set[str] = set(output_for_vehicle.iter_over_zone_ids())
        if not zone_ids_in_output.issubset(zone_ids_in_input):
            raise RuntimeError("Inconsistency between input (zone exit events passed) "
                               "and output (in-zone motion status change events): "
                               "all zone IDs from output are expected to be contained in input, "
                               "but got zone IDs that are not in input")

    def _check_input_against_output_for_dead_vehicle(
            self, *, input_for_vehicle: InputEventsForDeadVehicle,
            output_for_vehicle: InZoneMotionStatusPeriodBoundaryEventsIntermediaryContainer
    ) -> None:
        # runtime checks on logical consistency
        self._check_for_dead_vehicle_end_output_events_only(output_for_vehicle)
        self._check_for_dead_vehicle_all_output_zone_ids_in_input(input_for_vehicle=input_for_vehicle,
                                                                  output_for_vehicle=output_for_vehicle)

    def _update_and_get_events_for_dead_vehicles(
            self, input_events_by_vehicle: List[InputForDeadVehicleWithVehicleID],
            *, cur_frame_ts: datetime
    ) -> InZoneMotionStatusPeriodBoundaryEventsIntermediaryContainer:
        """
        Get events for dead vehicles and remove their updaters. Used when the vehicle's lifetime ends
        and processing in this updater goes on (not to ensure a graceful shutdown when the global processing ends).

        The passed current frame's timestamp will be used for the events emitted,
        and their `truncated` attribute will be set to `False`.
        """
        containers: List[InZoneMotionStatusPeriodBoundaryEventsIntermediaryContainer] = []
        for input_for_vehicle in input_events_by_vehicle:  # type: InputForDeadVehicleWithVehicleID
            vehicle_id: str = input_for_vehicle.vehicle_id
            # select the updater for this vehicle
            updater: VehicleStatusUpdater = self._state.get_updater_for_vehicle(vehicle_id)
            # the vehicle is now dead -- end the processing for this updater and get end events
            vehicle_output: InZoneMotionStatusPeriodBoundaryEventsIntermediaryContainer = (
                updater.end_processing_and_get_events_with_true_period_end(cur_frame_ts)
            )
            # a runtime check for consistency
            self._check_input_against_output_for_dead_vehicle(input_for_vehicle=input_for_vehicle.events,
                                                              output_for_vehicle=vehicle_output)
            # append to the list
            containers.append(vehicle_output)
            # deregister this vehicle ID
            self._state.remove_vehicle(vehicle_id)
        return InZoneMotionStatusPeriodBoundaryEventsIntermediaryContainer.concatenate(containers)

    def _run_updaters_and_get_events(
            self, input_events_by_vehicle: InputForVehicles, *, system_state: ProcessingSystemState
    ) -> InZoneMotionStatusPeriodBoundaryEventsContainer:
        output_for_alive_vehicles: InZoneMotionStatusPeriodBoundaryEventsIntermediaryContainer = (
            self._update_and_get_events_for_alive_vehicles(
                input_events_by_vehicle.alive, system_state=system_state
            )
        )
        output_for_dead_vehicles: InZoneMotionStatusPeriodBoundaryEventsIntermediaryContainer = (
            self._update_and_get_events_for_dead_vehicles(
                input_events_by_vehicle.dead, cur_frame_ts=system_state.cur_frame_ts
            )
        )
        output_for_all_vehicles: InZoneMotionStatusPeriodBoundaryEventsIntermediaryContainer = (
            InZoneMotionStatusPeriodBoundaryEventsIntermediaryContainer.concatenate(
                (output_for_alive_vehicles, output_for_dead_vehicles)
            )
        )
        out_container: InZoneMotionStatusPeriodBoundaryEventsContainer = InZoneMotionStatusPeriodBoundaryEventsContainer(
            start=InZoneMotionStatusStartEventsContainer(stationary=output_for_all_vehicles.start.stationary,
                                                         moving=output_for_all_vehicles.start.moving),
            end=InZoneMotionStatusEndEventsContainer(stationary=output_for_all_vehicles.end.stationary,
                                                     moving=output_for_all_vehicles.end.moving)
        )
        return out_container

    @staticmethod
    def _check_input_and_arrange_by_vehicle(
            *, global_motion_events: ConfirmedMotionStatusUpdatesContainer, zone_events: ZoneTransitEventsContainer
    ) -> InputForVehicles:
        """
        Create a wrapper for input events organised by vehicles that are alive vs dead;
        and inside each item in either type:
        (1) vehicle id -- for both;
        (2a) zone entrance/exit events -- for alive vehicles;
        (2b) zone exit events only -- for dead vehicles.
        """
        # vehicle_id -> EventsForVehicle
        mappings_for_alive: Dict[str, InputEventsForAliveVehicle] = dict()
        mappings_for_dead: Dict[str, InputEventsForDeadVehicle] = dict()

        # populate with motion status updates
        for motion_update in global_motion_events.updates: # type: ConfirmedMotionStatusUpdate
            # motion status update present -> vehicle alive
            motion_update_vehicle_id: str = motion_update.vehicle_id
            if motion_update_vehicle_id in mappings_for_alive:
                # there must be at most one stationary event for a given vehicle id
                raise ValueError(
                    f"Global confirmed motion update events must have unique vehicle IDs, "
                    f"found duplicates for {motion_update_vehicle_id}"
                )
            # map this motion update to this vehicle id
            # and create a new empty zone occupancy events container for it
            mappings_for_alive[motion_update_vehicle_id] = InputEventsForAliveVehicle(
                cur_motion_status_update=motion_update,
                zone_occupancy_events=ZoneTransitEventsIntermediaryContainer()
            )
        # populate with zone entrance events
        for zone_start_event in zone_events.start: # type: ZoneEntranceEvent
            zone_start_event_vehicle_id: str = zone_start_event.vehicle_id
            if zone_start_event_vehicle_id not in mappings_for_alive:
                # by design, should never happen: zone start events are only generated for alive vehicles,
                # but motion status updates are generated for all vehicles that are alive
                raise ValueError("Encountered a zone entrance event with vehicle ID not present "
                                 "in global confirmed motion update events passed")
            mappings_for_alive[zone_start_event_vehicle_id].zone_occupancy_events.start.append(zone_start_event)
        # populate with zone exit events
        for zone_exit_event in zone_events.end: # type: ZoneExitEvent
            zone_exit_event_vehicle_id: str = zone_exit_event.vehicle_id
            if zone_exit_event_vehicle_id in mappings_for_alive:
                # alive vehicle
                mappings_for_alive[zone_exit_event_vehicle_id].zone_occupancy_events.end.append(zone_exit_event)
            else:
                # dead vehicle
                if zone_exit_event_vehicle_id not in mappings_for_dead:
                    mappings_for_dead[zone_exit_event_vehicle_id] = InputEventsForDeadVehicle(zone_exit_events=[])
                mappings_for_dead[zone_exit_event_vehicle_id].zone_exit_events.append(zone_exit_event)
        # wrap into containers containing vehicle IDs
        containers_for_alive: List[InputForAliveVehicleWithVehicleID] = [
            InputForAliveVehicleWithVehicleID(vehicle_id=vehicle_id,
                                              events=inner_container)
            for vehicle_id, inner_container in mappings_for_alive.items()
        ]
        containers_for_dead: List[InputForDeadVehicleWithVehicleID] = [
            InputForDeadVehicleWithVehicleID(vehicle_id=vehicle_id,
                                             events=inner_container)
            for vehicle_id, inner_container in mappings_for_dead.items()
        ]
        return InputForVehicles(alive=containers_for_alive,
                                dead=containers_for_dead)

    @override
    def _update_and_get_events(
            self, input_obj: InZoneMotionStatusPeriodBoundaryEventGeneratorInput, *, system_state: ProcessingSystemState
    ) -> InZoneMotionStatusPeriodBoundaryEventsContainer:
        self._add_updaters_for_new_vehicles(input_obj)

        # arrange stationary and zone events by vehicle id
        input_events_by_vehicle: InputForVehicles = self._check_input_and_arrange_by_vehicle(
            global_motion_events=input_obj.global_motion_status_updates,
            zone_events=input_obj.zone_transit_events
        )
        # produce events and update the updaters
        container: InZoneMotionStatusPeriodBoundaryEventsContainer = self._run_updaters_and_get_events(
            input_events_by_vehicle, system_state=system_state
        )

        # updaters for dead vehicles are now removed in `_update_and_get_events_for_dead_vehicles`,
        # because their end processing method is being called there
        # and the updaters should not be kept alive after that anyway

        # self._remove_updaters_for_dead_vehicles(input_obj.lifetime_events.end)

        return container

    @override
    def _get_events_for_end_of_processing(self,
                                          *, event_ts: datetime,
                                          truncate_events: bool) -> InZoneMotionStatusPeriodBoundaryEventsContainer:
        output_events: InZoneMotionStatusPeriodBoundaryEventsIntermediaryContainer = (
            InZoneMotionStatusPeriodBoundaryEventsIntermediaryContainer()
        )
        containers: List[InZoneMotionStatusPeriodBoundaryEventsIntermediaryContainer] = []
        for vehicle_id in self._state.get_vehicle_ids(): # type: str
            updater: VehicleStatusUpdater = self._state.get_updater_for_vehicle(vehicle_id)
            # update the state and get output events for this vehicle
            vehicle_output: InZoneMotionStatusPeriodBoundaryEventsIntermediaryContainer = (
                # get events (using the previous frame's timestamp for the end events
                # and with `truncated` set to `True`)
                updater.end_processing_and_get_events_with_truncation()
            )
            containers.append(vehicle_output)
        out_container: InZoneMotionStatusPeriodBoundaryEventsContainer = output_events.to_final_container()
        return out_container

