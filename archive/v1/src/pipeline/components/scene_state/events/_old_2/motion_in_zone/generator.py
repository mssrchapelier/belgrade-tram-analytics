from typing import List, Set, Dict, Iterable, Collection, NamedTuple
from dataclasses import dataclass, field

from tram_analytics.v1.models.components.scene_state.events.base import EventBoundaryType
from tram_analytics.v1.models.components.scene_state.events.subprocessors.zone_transit import ZoneEntranceEvent, ZoneExitEvent, \
    ZoneTransitEventsContainer
from tram_analytics.v1.models.components.scene_state.events.subprocessors.lifetime import VehicleLifetimeStartEvent, VehicleLifetimeEndEvent, \
    VehiclesLifetimeEventsContainer
from archive.v1.src.pipeline.components.scene_state.events._old_2.motion_global.events import StationaryEvent_Old, StationaryEventsContainer_Old
from archive.v1.src.pipeline.components.scene_state.events._old_2.motion_in_zone.events import (
    InZoneStationaryStartEvent, InZoneStationaryEndEvent, InZoneStationaryEventsContainer
)

# --- auxiliary containers for events ---

# --- (1) input ---

@dataclass(slots=True, kw_only=True)
class IntermediaryZoneEventsContainer:
    start: List[ZoneEntranceEvent] = field(default_factory=list)
    end: List[ZoneExitEvent] = field(default_factory=list)

class InputEventsForVehicle(NamedTuple):
    # all events present for a single vehicle in the update
    stationary: StationaryEvent_Old | None
    zones: IntermediaryZoneEventsContainer

class VehicleIdAndEvents(NamedTuple):
    vehicle_id: str
    events: InputEventsForVehicle

# --- (2) output ---

@dataclass(slots=True, kw_only=True)
class IntermediaryOutputEventsContainer:
    start: List[InZoneStationaryStartEvent] = field(default_factory=list)
    end: List[InZoneStationaryEndEvent] = field(default_factory=list)

# --- status updater for a single vehicle ---

class VehicleStatusUpdater:

    """
    For a single vehicle, stores its stationary status and the zone IDs to which the vehicle is currently assigned,
    and updates them based on input events (the stationary event, if any, and zone entrance/exit events, if any),
    if any, received for this vehicle.
    """

    def __init__(self, vehicle_id: str) -> None:
        self._vehicle_id: str = vehicle_id
        self._prev_is_stationary: bool | None = None
        self._prev_zone_ids: Set[str] = set()

    @staticmethod
    def _check_input(input_for_vehicle: InputEventsForVehicle) -> None:
        stat_event: StationaryEvent_Old | None = input_for_vehicle.stationary
        zone_events: IntermediaryZoneEventsContainer = input_for_vehicle.zones

        # (1) all events must have the same vehicle id
        vehicle_ids_present: Set[str] = set()
        if stat_event is not None:
            # add stat event vehicle id
            vehicle_ids_present.add(stat_event.vehicle_id)
        # add zone events vehicle ids
        vehicle_ids_present.update({e.vehicle_id for e in (*zone_events.start, *zone_events.end)})

        if not len(vehicle_ids_present) in (0, 1):
            differing_vehicle_id_error_msg: str = "All events must have the same vehicle_id, found more than one: {}".format(
                ", ".join(vehicle_ids_present)
            )
            raise ValueError(differing_vehicle_id_error_msg)

        # (2) all zone events must have unique zone ids
        zone_ids: Set[str] = {e.zone_id for e in (*zone_events.start, *zone_events.end)}
        total_zone_ids: int = len(zone_ids)
        total_events: int = len(zone_events.start) + len(zone_events.end)
        if total_zone_ids != total_events:
            nonunique_zone_ids_msg: str = "All zone_ids in zone_events must be unique"
            raise ValueError(nonunique_zone_ids_msg)

    def _compute_cur_is_stationary(self, stationary_event: StationaryEvent_Old | None):
        cur_is_stationary: bool | None = self._prev_is_stationary
        if stationary_event is not None:
            match stationary_event.boundary_type:
                case EventBoundaryType.START:
                    cur_is_stationary = True
                case EventBoundaryType.END:
                    cur_is_stationary = False
                case _:
                    raise ValueError(f"Unknown event boundary type: {stationary_event.boundary_type}")
        return cur_is_stationary

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

    def _produce_events_for_zone_changes(self, *, zone_events: IntermediaryZoneEventsContainer,
                                         cur_stationary: bool | None) -> IntermediaryOutputEventsContainer:
        # produce in-zone stationary events for zone entrances/exits
        # (depending on whether the vehicle is stationary, was stationary)

        container: IntermediaryOutputEventsContainer = IntermediaryOutputEventsContainer()

        for entrance_event in zone_events.start: # type: ZoneEntranceEvent
            # entered zone ...
            if cur_stationary:
                # ... and is stationary
                container.start.append(
                    InZoneStationaryStartEvent(vehicle_id=entrance_event.vehicle_id,
                                               zone_id=entrance_event.zone_id)
                )
        for exit_event in zone_events.end: # type: ZoneExitEvent
            # exited zone ...
            if self._prev_is_stationary:
                # ... and was stationary
                container.end.append(
                    InZoneStationaryEndEvent(vehicle_id=exit_event.vehicle_id,
                                             zone_id=exit_event.zone_id)
                )

        return container


    def _produce_events_for_unchanged_zones(self, *, unchanged_zone_ids: Iterable[str],
                                            cur_is_stationary: bool | None) -> IntermediaryOutputEventsContainer:
        # produce in-zone stationary events for unchanged assigned zones
        # (depending on whether the vehicle is stationary, was stationary)
        container: IntermediaryOutputEventsContainer = IntermediaryOutputEventsContainer()
        for zone_id in unchanged_zone_ids: # type: str
            if self._prev_is_stationary and cur_is_stationary is False:
                # was stationary, is moving
                # NOTE: not the same as: was stationary, is undefined --> do nothing in that case
                container.end.append(
                    InZoneStationaryEndEvent(vehicle_id=self._vehicle_id, zone_id=zone_id)
                )
            elif not self._prev_is_stationary and cur_is_stationary:
                # was moving or undefined, is stationary
                container.start.append(
                    InZoneStationaryStartEvent(vehicle_id=self._vehicle_id, zone_id=zone_id)
                )
        return container

    def update_and_get_events(self, input_for_vehicle: InputEventsForVehicle) -> IntermediaryOutputEventsContainer:
        self._check_input(input_for_vehicle)

        # calculate whether the vehicle is currently stationary, moving, or undefined
        # (undefined is possible only before any change to it)
        cur_is_stationary: bool | None = self._compute_cur_is_stationary(input_for_vehicle.stationary)

        # calculate all zones to which the vehicle is currently assigned
        cur_zone_ids: Set[str] = self._compute_cur_zone_ids(input_for_vehicle.zones)
        # select zones to which the vehicle was already assigned
        unchanged_zone_ids: Set[str] = set.intersection(self._prev_zone_ids, cur_zone_ids)

        # produce in-zone stationary events for zone entrances/exits
        # (depending on whether the vehicle is stationary, was stationary)
        dest_events_for_zone_changes: IntermediaryOutputEventsContainer = self._produce_events_for_zone_changes(
            zone_events=input_for_vehicle.zones, cur_stationary=cur_is_stationary
        )
        # produce in-zone stationary events for unchanged assigned zones
        # (depending on whether the vehicle is stationary, was stationary)
        dest_events_for_unchanged_zones: IntermediaryOutputEventsContainer = self._produce_events_for_unchanged_zones(
            unchanged_zone_ids=unchanged_zone_ids, cur_is_stationary=cur_is_stationary
        )
        # build the output container with all events for this vehicle
        container: IntermediaryOutputEventsContainer = IntermediaryOutputEventsContainer(
            start=[*dest_events_for_zone_changes.start,
                   *dest_events_for_unchanged_zones.start],
            end=[*dest_events_for_zone_changes.end,
                 *dest_events_for_unchanged_zones.end]
        )

        # update this vehicle's state
        self._prev_is_stationary = cur_is_stationary
        self._prev_zone_ids = cur_zone_ids

        return container

# --- event generator ---

class StationaryInZoneEventGenerator:

    def __init__(self) -> None:
        # for all vehicles alive:
        # vehicle id -> vehicle updater
        self._vehicle_updaters: Dict[str, VehicleStatusUpdater] = dict()

    @staticmethod
    def _check_input_and_arrange_by_vehicle(
            *, stationary_events: StationaryEventsContainer_Old, zone_events: ZoneTransitEventsContainer
    ) -> List[VehicleIdAndEvents]:
        """
        Create a list of items where each item contains events pertinent to a single vehicle,
        namely at least one of the following:
        (1) at most one global stationary event;
        (2) zone entrance/exit events.
        """
        # vehicle_id -> EventsForVehicle
        mappings: Dict[str, InputEventsForVehicle] = dict()

        stat_events: List[StationaryEvent_Old] = [*stationary_events.start, *stationary_events.end]
        for stat_event in stat_events:  # type: StationaryEvent_Old
            stat_event_vehicle_id: str = stat_event.vehicle_id
            if stat_event_vehicle_id in mappings:
                # there must be at most one stationary event for a given vehicle id
                raise ValueError(
                    f"stationary_events must have unique vehicle IDs, found duplicates for {stat_event_vehicle_id}"
                )
            # map this stationary event to this vehicle id
            # and create a new zone occupancy events container for it
            mappings[stat_event_vehicle_id] = InputEventsForVehicle(
                stationary=stat_event,
                zones=IntermediaryZoneEventsContainer()
            )

        for zone_start_event in zone_events.start: # type: ZoneEntranceEvent
            zone_start_event_vehicle_id: str = zone_start_event.vehicle_id
            if zone_start_event_vehicle_id in mappings:
                # (stationary event also present for this vehicle)
                mappings[zone_start_event_vehicle_id].zones.start.append(zone_start_event)
            else:
                # (stationary event not present for this vehicle)
                mappings[zone_start_event_vehicle_id] = InputEventsForVehicle(
                    stationary=None,
                    zones=IntermediaryZoneEventsContainer(
                        start=[zone_start_event]
                    )
                )
        for zone_end_event in zone_events.end: # type: ZoneExitEvent
            zone_end_event_vehicle_id: str = zone_end_event.vehicle_id
            if zone_end_event_vehicle_id in mappings:
                # (stationary event or start zone event also present for this vehicle)
                mappings[zone_end_event_vehicle_id].zones.end.append(zone_end_event)
            else:
                # (neither stationary nor start zone event event present for this vehicle)
                mappings[zone_end_event_vehicle_id] = InputEventsForVehicle(
                    stationary=None,
                    zones=IntermediaryZoneEventsContainer(
                        end=[zone_end_event]
                    )
                )
        return [VehicleIdAndEvents(vehicle_id=vehicle_id, events=container)
                for vehicle_id, container in mappings.items()]

    def _add_updaters_for_new_vehicles(self, lifetime_start_events: Iterable[VehicleLifetimeStartEvent]) -> None:
        # call BEFORE any processing of stationary and zone events, to initialise the updaters for new vehicles
        for event in lifetime_start_events:
            vehicle_id: str = event.vehicle_id
            if vehicle_id in self._vehicle_updaters:
                raise ValueError(f"Got a lifetime start event for an already registered vehicle: {vehicle_id}")
            self._vehicle_updaters[vehicle_id] = VehicleStatusUpdater(vehicle_id)

    def _remove_updaters_for_dead_vehicles(self, lifetime_end_events: Iterable[VehicleLifetimeEndEvent]) -> None:
        # call AFTER any processing of stationary and zone events, to delete the updaters for dead vehicles
        for event in lifetime_end_events:
            vehicle_id: str = event.vehicle_id
            if vehicle_id not in self._vehicle_updaters:
                raise ValueError(f"Got a lifetime end event for a vehicle that was not registered: {vehicle_id}")
            self._vehicle_updaters.pop(vehicle_id)

    def _run_updaters_and_get_events(
            self, input_events_by_vehicle: Collection[VehicleIdAndEvents]
    ) -> InZoneStationaryEventsContainer:
        output_events: IntermediaryOutputEventsContainer = IntermediaryOutputEventsContainer()
        for input_for_vehicle in input_events_by_vehicle: # type: VehicleIdAndEvents
            # select the updater for this vehicle
            updater: VehicleStatusUpdater = self._vehicle_updaters[input_for_vehicle.vehicle_id]
            # update the state and get output events for this vehicle
            output_for_vehicle: IntermediaryOutputEventsContainer = updater.update_and_get_events(
                input_for_vehicle.events
            )
            # add results to output container
            output_events.start.extend(output_for_vehicle.start)
            output_events.end.extend(output_for_vehicle.end)
        out_container: InZoneStationaryEventsContainer = InZoneStationaryEventsContainer(
            start=output_events.start,
            end=output_events.end
        )
        return out_container

    def update_and_get_events(
            self, *, lifetime_events: VehiclesLifetimeEventsContainer,
            global_stationary_events: StationaryEventsContainer_Old,
            zone_occupancy_events: ZoneTransitEventsContainer
    ) -> InZoneStationaryEventsContainer:
        self._add_updaters_for_new_vehicles(lifetime_events.start)

        # arrange stationary and zone events by vehicle id
        input_events_by_vehicle: List[VehicleIdAndEvents] = self._check_input_and_arrange_by_vehicle(
            stationary_events=global_stationary_events,
            zone_events=zone_occupancy_events
        )
        # produce events and update the updaters
        container: InZoneStationaryEventsContainer = self._run_updaters_and_get_events(input_events_by_vehicle)

        self._remove_updaters_for_dead_vehicles(lifetime_events.end)

        return container

