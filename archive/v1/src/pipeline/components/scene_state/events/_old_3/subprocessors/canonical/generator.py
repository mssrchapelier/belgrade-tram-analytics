from typing import List, Dict, override, Type, Set, Iterable
from dataclasses import dataclass, field
from datetime import datetime

from tram_analytics.v1.models.common_types import VehicleType
from tram_analytics.v1.models.components.scene_state.events.base import generate_event_uuid, generate_period_uuid
from archive.v1.src.pipeline.components.scene_state.events._old_3.subprocessors.base.base_generator import (
    BaseFinalEventGenerator, ProcessingSystemState
)
from archive.v1.src.pipeline.components.scene_state.events._old_3.subprocessors.base.base_generator import BaseGeneratorState
from tram_analytics.v1.pipeline.components.scene_state.events.subprocessors.base.base_types import VehicleZoneMapping, VehicleIdTypeMapping
from tram_analytics.v1.pipeline.components.scene_state.events.pipeline.events_input import EventsInputData, VehicleInput
from archive.v1.src.pipeline.components.scene_state.events._old_3.subprocessors.canonical.events import (
    CanonicalEventsContainer
)
from tram_analytics.v1.models.components.scene_state.events.subprocessors.zone_transit import ZoneEntranceEvent, ZoneExitEvent, \
    ZoneTransitEventsContainer
from tram_analytics.v1.models.components.scene_state.events.subprocessors.lifetime import VehicleLifetimeStartEvent, VehicleLifetimeEndEvent, \
    VehiclesLifetimeEventsContainer
from tram_analytics.v1.models.components.scene_state.events.subprocessors.speed import SpeedsWrapper, SpeedUpdateEvent


@dataclass(frozen=True, slots=True, kw_only=True)
class CanonicalGeneratorStateSnapshot:
    vehicle_ids: Set[str]
    vehicle_id_to_type: Dict[str, VehicleType]
    vehicle_id_to_lifetime_period_id: Dict[str, str]
    vehicle_zone_mappings: Set[VehicleZoneMapping]
    vehicle_zone_mapping_to_transit_period_id: Dict[VehicleZoneMapping, str]

class CanonicalGeneratorState(BaseGeneratorState):

    def __init__(self) -> None:
        # vehicle IDs of vehicles that are alive
        self._vehicle_ids: Set[str] = set()
        # vehicle id -> vehicle type
        self._vehicle_id_to_type: Dict[str, VehicleType] = dict()
        # vehicle id -> lifetime period ID
        self._vehicle_id_to_lifetime_period_id: Dict[str, str] = dict()
        # (vehicle ID, zone ID) tuples for vehicles that are currently in a specific zone
        self._vehicle_zone_mappings: Set[VehicleZoneMapping] = set()
        # (vehicle ID, zone ID) -> zone occupancy period ID
        self._vehicle_zone_mapping_to_transit_period_id: Dict[VehicleZoneMapping, str] = dict()

    def generate_snapshot(self) -> CanonicalGeneratorStateSnapshot:
        return CanonicalGeneratorStateSnapshot(
            vehicle_ids=self._vehicle_ids.copy(),
            vehicle_id_to_type=self._vehicle_id_to_type.copy(),
            vehicle_id_to_lifetime_period_id=self._vehicle_id_to_lifetime_period_id.copy(),
            vehicle_zone_mappings=self._vehicle_zone_mappings.copy(),
            vehicle_zone_mapping_to_transit_period_id=self._vehicle_zone_mapping_to_transit_period_id.copy()
        )

    @property
    def vehicle_ids(self) -> Set[str]:
        return self._vehicle_ids.copy()

    @property
    def vehicle_zone_mappings(self) -> Set[VehicleZoneMapping]:
        return self.vehicle_zone_mappings.copy()

    def get_vehicle_type(self, vehicle_id: str) -> VehicleType:
        if vehicle_id not in self._vehicle_id_to_type:
            raise ValueError(f"Can't get vehicle type for vehicle ID {vehicle_id}: "
                             f"not registered in _vehicle_id_to_type")
        return self._vehicle_id_to_type[vehicle_id]

    def get_lifetime_period_id_for_vehicle(self, vehicle_id: str) -> str:
        if vehicle_id not in self._vehicle_id_to_lifetime_period_id:
            raise ValueError(f"Can't get lifetime period ID for vehicle ID {vehicle_id}: "
                             f"not registered in _vehicle_id_to_lifetime_period_id")
        return self._vehicle_id_to_lifetime_period_id[vehicle_id]

    def get_transit_period_id_for_vehicle_zone_mapping(self, mapping: VehicleZoneMapping) -> str:
        if mapping not in self._vehicle_zone_mapping_to_transit_period_id:
            raise ValueError(f"Can't get occupancy period ID for vehicle ID {mapping.vehicle_id} "
                             f"and zone ID {mapping.zone_id}: "
                             "not registered in _vehicle_zone_mapping_to_occupancy_period_id")
        return self._vehicle_zone_mapping_to_transit_period_id[mapping]

    @override
    def clear(self) -> None:
        self._vehicle_ids.clear()
        self._vehicle_id_to_type.clear()
        self._vehicle_id_to_lifetime_period_id.clear()
        self._vehicle_zone_mappings.clear()
        self._vehicle_zone_mapping_to_transit_period_id.clear()

    def _check_vehicle_not_registered(self, vehicle_id: str) -> None:
        if vehicle_id in self._vehicle_ids:
            raise ValueError(f"Vehicle already registered in vehicle_ids: {vehicle_id}")
        if vehicle_id in self._vehicle_id_to_type:
            raise ValueError(f"Vehicle already registered in vehicle_id_to_type: {vehicle_id}")
        if vehicle_id in self._vehicle_id_to_lifetime_period_id:
            raise ValueError(f"Vehicle already registered in vehicle_id_to_lifetime_period_id: {vehicle_id}")

    def _check_vehicle_is_registered(self, vehicle_id: str):
        if vehicle_id not in self._vehicle_ids:
            raise ValueError(f"Vehicle not registered in vehicle_ids: {vehicle_id}")
        if vehicle_id not in self._vehicle_id_to_type:
            raise ValueError(f"Vehicle not registered in vehicle_id_to_type: {vehicle_id}")
        if vehicle_id not in self._vehicle_id_to_lifetime_period_id:
            raise ValueError(f"Vehicle not registered in vehicle_id_to_lifetime_period_id: {vehicle_id}")

    def _check_vehicle_zone_mapping_not_registered(self, mapping: VehicleZoneMapping) -> None:
        if mapping in self._vehicle_zone_mappings:
            raise ValueError(f"Mapping (vehicle ID: {mapping.vehicle_id}, zone ID: {mapping.zone_id}) "
                             f"already registered in vehicle_zone_mappings")
        if mapping in self._vehicle_zone_mapping_to_transit_period_id:
            raise ValueError(f"Mapping (vehicle ID: {mapping.vehicle_id}, zone ID: {mapping.zone_id}) "
                             f"already registered in _vehicle_zone_mapping_to_transit_period_id")

    def _check_vehicle_zone_mapping_is_registered(self, mapping: VehicleZoneMapping) -> None:
        if mapping not in self._vehicle_zone_mappings:
            raise ValueError(f"Mapping (vehicle ID: {mapping.vehicle_id}, zone ID: {mapping.zone_id}) "
                             f"not registered in vehicle_zone_mappings")
        if mapping not in self._vehicle_zone_mapping_to_transit_period_id:
            raise ValueError(f"Mapping (vehicle ID: {mapping.vehicle_id}, zone ID: {mapping.zone_id}) "
                             f"not registered in _vehicle_zone_mapping_to_transit_period_id")

    def add_vehicle(self,
                    *, vehicle_id: str,
                    vehicle_type: VehicleType,
                    lifetime_period_id: str) -> None:
        self._check_vehicle_not_registered(vehicle_id)

        self._vehicle_ids.add(vehicle_id)
        self._vehicle_id_to_type[vehicle_id] = vehicle_type
        self._vehicle_id_to_lifetime_period_id[vehicle_id] = lifetime_period_id

    def remove_vehicle_and_get_lifetime_period_id(self, vehicle_id: str) -> str:
        self._check_vehicle_is_registered(vehicle_id)

        lifetime_period_id: str = self._vehicle_id_to_lifetime_period_id[vehicle_id]

        self._vehicle_ids.remove(vehicle_id)
        self._vehicle_id_to_type.pop(vehicle_id)
        self._vehicle_id_to_lifetime_period_id.pop(vehicle_id)

        return lifetime_period_id

    def register_zone_entrance(self, *, vehicle_id: str, zone_id: str,
                               occupancy_period_id: str) -> None:
        mapping: VehicleZoneMapping = VehicleZoneMapping(vehicle_id=vehicle_id, zone_id=zone_id)
        # self._check_vehicle_is_registered(vehicle_id)
        self._check_vehicle_zone_mapping_not_registered(mapping)

        self._vehicle_zone_mappings.add(mapping)
        self._vehicle_zone_mapping_to_transit_period_id[mapping] = occupancy_period_id

    def register_zone_exit_and_get_transit_period_id(self, *, vehicle_id: str,
                                                     zone_id: str) -> str:
        mapping: VehicleZoneMapping = VehicleZoneMapping(vehicle_id=vehicle_id, zone_id=zone_id)
        self._check_vehicle_zone_mapping_is_registered(mapping)

        transit_period_id: str = self._vehicle_zone_mapping_to_transit_period_id[mapping]

        self._vehicle_zone_mappings.remove(mapping)
        self._vehicle_zone_mapping_to_transit_period_id.pop(mapping)

        return transit_period_id


@dataclass(frozen=True, slots=True, kw_only=True)
class GeneratorState_Old:
    # vehicle IDs of vehicles that are alive
    _vehicle_ids: Set[str] = field(default_factory=set)
    # vehicle id -> vehicle type
    _vehicle_id_to_type: Dict[str, VehicleType] = field(default_factory=dict)
    # vehicle id -> lifetime period ID
    _vehicle_id_to_lifetime_period_id: Dict[str, str] = field(default_factory=dict)
    # (vehicle ID, zone ID) tuples for vehicles that are currently in a specific zone
    _vehicle_zone_mappings: Set[VehicleZoneMapping] = field(default_factory=set)
    # (vehicle ID, zone ID) -> zone occupancy period ID
    _vehicle_zone_mapping_to_occupancy_period_id: Dict[VehicleZoneMapping, str] = field(default_factory=dict)

    @property
    def vehicle_ids(self) -> Set[str]:
        return self._vehicle_ids.copy()

    @property
    def vehicle_zone_mappings(self) -> Set[VehicleZoneMapping]:
        return self.vehicle_zone_mappings.copy()

    def get_lifetime_period_id_for_vehicle(self, vehicle_id: str) -> str:
        if vehicle_id not in self._vehicle_id_to_lifetime_period_id:
            raise ValueError(f"Can't get lifetime period ID for vehicle ID {vehicle_id}: "
                             f"not registered in _vehicle_id_to_lifetime_period_id")
        return self._vehicle_id_to_lifetime_period_id[vehicle_id]

    def _get_occupancy_period_id_for_vehicle_zone_mapping(self, mapping: VehicleZoneMapping) -> str:
        if mapping not in self._vehicle_zone_mapping_to_occupancy_period_id:
            raise ValueError(f"Can't get occupancy period ID for vehicle ID {mapping.vehicle_id} "
                             f"and zone ID {mapping.zone_id}: "
                             "not registered in _vehicle_zone_mapping_to_occupancy_period_id")
        return self._vehicle_zone_mapping_to_occupancy_period_id[mapping]

    def clear(self) -> None:
        self._vehicle_ids.clear()
        self._vehicle_id_to_type.clear()
        self._vehicle_id_to_lifetime_period_id.clear()
        self._vehicle_zone_mappings.clear()
        self._vehicle_zone_mapping_to_occupancy_period_id.clear()

    def _check_vehicle_not_registered(self, vehicle_id: str) -> None:
        if vehicle_id in self._vehicle_ids:
            raise ValueError(f"Vehicle already registered in vehicle_ids: {vehicle_id}")
        if vehicle_id in self._vehicle_id_to_type:
            raise ValueError(f"Vehicle already registered in vehicle_id_to_type: {vehicle_id}")
        if vehicle_id in self._vehicle_id_to_lifetime_period_id:
            raise ValueError(f"Vehicle already registered in vehicle_id_to_lifetime_period_id: {vehicle_id}")

    def _check_vehicle_is_registered(self, vehicle_id: str):
        if vehicle_id not in self._vehicle_ids:
            raise ValueError(f"Vehicle not registered in vehicle_ids: {vehicle_id}")
        if vehicle_id not in self._vehicle_id_to_type:
            raise ValueError(f"Vehicle not registered in vehicle_id_to_type: {vehicle_id}")
        if vehicle_id not in self._vehicle_id_to_lifetime_period_id:
            raise ValueError(f"Vehicle not registered in vehicle_id_to_lifetime_period_id: {vehicle_id}")

    def _check_vehicle_zone_mapping_not_registered(self, mapping: VehicleZoneMapping) -> None:
        if mapping in self._vehicle_zone_mappings:
            raise ValueError(f"Mapping (vehicle ID: {mapping.vehicle_id}, zone ID: {mapping.zone_id}) "
                             f"already registered in vehicle_zone_mappings")
        if mapping in self._vehicle_zone_mapping_to_occupancy_period_id:
            raise ValueError(f"Mapping (vehicle ID: {mapping.vehicle_id}, zone ID: {mapping.zone_id}) "
                             f"already registered in vehicle_zone_mapping_to_occupancy_period_id")

    def _check_vehicle_zone_mapping_is_registered(self, mapping: VehicleZoneMapping) -> None:
        if mapping not in self._vehicle_zone_mappings:
            raise ValueError(f"Mapping (vehicle ID: {mapping.vehicle_id}, zone ID: {mapping.zone_id}) "
                             f"not registered in vehicle_zone_mappings")
        if mapping not in self._vehicle_zone_mapping_to_occupancy_period_id:
            raise ValueError(f"Mapping (vehicle ID: {mapping.vehicle_id}, zone ID: {mapping.zone_id}) "
                             f"not registered in vehicle_zone_mapping_to_occupancy_period_id")

    def add_vehicle(self,
                    *, vehicle_id: str,
                    vehicle_type: VehicleType,
                    lifetime_period_id: str) -> None:
        self._check_vehicle_not_registered(vehicle_id)

        self._vehicle_ids.add(vehicle_id)
        self._vehicle_id_to_type[vehicle_id] = vehicle_type
        self._vehicle_id_to_lifetime_period_id[vehicle_id] = lifetime_period_id

    def remove_vehicle_and_get_lifetime_period_id(self, vehicle_id: str) -> str:
        self._check_vehicle_is_registered(vehicle_id)

        lifetime_period_id: str = self._vehicle_id_to_lifetime_period_id[vehicle_id]

        self._vehicle_ids.remove(vehicle_id)
        self._vehicle_id_to_type.pop(vehicle_id)
        self._vehicle_id_to_lifetime_period_id.pop(vehicle_id)

        return lifetime_period_id

    def register_zone_entrance(self, *, vehicle_id: str, zone_id: str,
                               occupancy_period_id: str) -> None:
        mapping: VehicleZoneMapping = VehicleZoneMapping(vehicle_id=vehicle_id, zone_id=zone_id)
        # self._check_vehicle_is_registered(vehicle_id)
        self._check_vehicle_zone_mapping_not_registered(mapping)

        self._vehicle_zone_mappings.add(mapping)
        self._vehicle_zone_mapping_to_occupancy_period_id[mapping] = occupancy_period_id

    def register_zone_exit_and_get_lifetime_period_id(self, *, vehicle_id: str,
                                                      zone_id: str) -> str:
        mapping: VehicleZoneMapping = VehicleZoneMapping(vehicle_id=vehicle_id, zone_id=zone_id)
        self._check_vehicle_zone_mapping_is_registered(mapping)

        occupancy_period_id: str = self._vehicle_zone_mapping_to_occupancy_period_id[mapping]

        self._vehicle_zone_mappings.remove(mapping)
        self._vehicle_zone_mapping_to_occupancy_period_id.pop(mapping)

        return occupancy_period_id

class CanonicalEventGenerator(BaseFinalEventGenerator[EventsInputData, CanonicalEventsContainer]):

    def __init__(self) -> None:
        super().__init__()
        # mutated during processing
        self._state: CanonicalGeneratorState = CanonicalGeneratorState()

    @override
    @classmethod
    def _get_container_class(cls) -> Type[CanonicalEventsContainer]:
        return CanonicalEventsContainer

    def _check_vehicle_type_has_not_changed(self, *, vehicle_id: str, cur_vehicle_type: VehicleType) -> None:
        prev_vehicle_type: VehicleType = self._state.get_vehicle_type(vehicle_id)
        if cur_vehicle_type is not prev_vehicle_type:
            raise ValueError(f"Vehicle type changes are not allowed: "
                             f"stored {prev_vehicle_type}, got {cur_vehicle_type}")

    def _add_new_vehicles_and_get_events(
            self, mappings: Iterable[VehicleIdTypeMapping], *, system_state: ProcessingSystemState
    ) -> List[VehicleLifetimeStartEvent]:
        events: List[VehicleLifetimeStartEvent] = []
        for mapping in mappings: # type: VehicleIdTypeMapping
            vehicle_id: str = mapping.vehicle_id
            vehicle_type: VehicleType = mapping.vehicle_type
            lifetime_period_id: str = generate_period_uuid()
            self._state.add_vehicle(vehicle_id=vehicle_id,
                                    vehicle_type=vehicle_type,
                                    lifetime_period_id=lifetime_period_id)
            event: VehicleLifetimeStartEvent = VehicleLifetimeStartEvent(
                event_id=generate_event_uuid(),
                period_id=lifetime_period_id,
                vehicle_id=vehicle_id,
                vehicle_type=vehicle_type,
                event_ts=system_state.cur_frame_ts,
                truncated=system_state.is_first_frame
            )
            events.append(event)
        return events

    def _remove_dead_vehicles_and_get_events(
            self, dead_vehicle_ids: Iterable[str], *, event_ts: datetime, truncate: bool
    ) -> List[VehicleLifetimeEndEvent]:
        events: List[VehicleLifetimeEndEvent] = []
        for vehicle_id in dead_vehicle_ids: # type: str
            lifetime_period_id: str = self._state.remove_vehicle_and_get_lifetime_period_id(vehicle_id)
            event: VehicleLifetimeEndEvent = VehicleLifetimeEndEvent(
                event_id=generate_event_uuid(),
                period_id=lifetime_period_id,
                vehicle_id=vehicle_id,
                event_ts=event_ts,
                truncated=truncate
            )
            events.append(event)
        return events

    def _register_zone_entrances_and_get_events(
            self, mappings: Iterable[VehicleZoneMapping], *, system_state: ProcessingSystemState
    ) -> List[ZoneEntranceEvent]:
        events: List[ZoneEntranceEvent] = []
        for mapping in mappings: # type: VehicleZoneMapping
            vehicle_id: str = mapping.vehicle_id
            zone_id: str = mapping.zone_id
            transit_period_id: str = generate_period_uuid()
            self._state.register_zone_entrance(vehicle_id=vehicle_id,
                                               zone_id=zone_id,
                                               occupancy_period_id=transit_period_id)
            event: ZoneEntranceEvent = ZoneEntranceEvent(
                event_id=generate_event_uuid(),
                event_ts=system_state.cur_frame_ts,
                truncated=system_state.is_first_frame,
                period_id=transit_period_id,
                vehicle_id=vehicle_id, zone_id=zone_id
            )
            events.append(event)
        return events

    def _register_zone_exits_and_get_events(
            self, mappings: Iterable[VehicleZoneMapping], *, event_ts: datetime, truncate: bool
    ) -> List[ZoneExitEvent]:
        events: List[ZoneExitEvent] = []
        for mapping in mappings: # type: VehicleZoneMapping
            vehicle_id: str = mapping.vehicle_id
            zone_id: str = mapping.zone_id
            transit_period_id: str = self._state.register_zone_exit_and_get_transit_period_id(
                vehicle_id=vehicle_id, zone_id=zone_id
            )
            event: ZoneExitEvent = ZoneExitEvent(
                event_id=generate_event_uuid(),
                event_ts=event_ts,
                truncated=truncate,
                period_id=transit_period_id,
                vehicle_id=vehicle_id, zone_id=zone_id
            )
            events.append(event)
        return events

    @override
    def _update_and_get_events(
            self, input_data: EventsInputData, *, system_state: ProcessingSystemState
    ) -> CanonicalEventsContainer:
        # (1) vehicle lifetime start/end
        # (2) vehicle zone entrance/exit
        # (3) vehicle speeds

        # vehicle id -> VehicleInput
        vehicle_id_to_input: Dict[str, VehicleInput] = {
            vehicle_input.vehicle_id: vehicle_input
            for vehicle_input in input_data.vehicles
        }

        prev_vehicle_ids: Set[str] = self._state.vehicle_ids
        alive_vehicle_ids: Set[str] = set(vehicle.vehicle_id for vehicle in input_data.vehicles)
        new_vehicle_ids: Set[str] = set.difference(alive_vehicle_ids, prev_vehicle_ids)
        dead_vehicle_ids: Set[str] = set.difference(prev_vehicle_ids, alive_vehicle_ids)

        prev_vehicle_zone_mappings: Set[VehicleZoneMapping] = self._state.vehicle_zone_mappings
        alive_vehicle_zone_mappings: Set[VehicleZoneMapping] = {
            VehicleZoneMapping(vehicle_id=vehicle.vehicle_id, zone_id=zone_id)
            for vehicle in input_data.vehicles
            for zone_id in vehicle.zone_ids
        }
        new_vehicle_zone_mappings: Set[VehicleZoneMapping] = set.difference(alive_vehicle_zone_mappings,
                                                                            prev_vehicle_zone_mappings)
        dead_vehicle_zone_mappings: Set[VehicleZoneMapping] = set.difference(prev_vehicle_zone_mappings,
                                                                             alive_vehicle_zone_mappings)

        speed_update_events: List[SpeedUpdateEvent] = []

        for vehicle in input_data.vehicles:  # type: VehicleInput
            vehicle_id: str = vehicle.vehicle_id
            cur_vehicle_type: VehicleType = vehicle.vehicle_type

            # check that the vehicle type has not changed
            if vehicle_id not in new_vehicle_ids:
                self._check_vehicle_type_has_not_changed(vehicle_id=vehicle_id,
                                                         cur_vehicle_type=cur_vehicle_type)

            # generate a speed update event
            speed_update: SpeedUpdateEvent = SpeedUpdateEvent(
                event_id=generate_event_uuid(),
                vehicle_id=vehicle_id,
                speeds=SpeedsWrapper(raw=vehicle.speeds.raw_ms,
                                     smoothed=vehicle.speeds.smoothed_ms),
                is_matched=vehicle.is_matched,
                event_ts=system_state.cur_frame_ts,
                truncated=system_state.is_first_frame
            )
            speed_update_events.append(speed_update)

        lifetime_start_events: List[VehicleLifetimeStartEvent] = self._add_new_vehicles_and_get_events(
            [VehicleIdTypeMapping(vehicle_id=v_id,
                                  vehicle_type=vehicle_id_to_input[v_id].vehicle_type)
             for v_id in new_vehicle_ids],
            system_state=system_state
        )
        lifetime_end_events: List[VehicleLifetimeEndEvent] = self._remove_dead_vehicles_and_get_events(
            dead_vehicle_ids, event_ts=system_state.cur_frame_ts, truncate=system_state.is_first_frame
        )
        zone_entrance_events: List[ZoneEntranceEvent] = self._register_zone_entrances_and_get_events(
            new_vehicle_zone_mappings, system_state=system_state
        )
        zone_exit_events: List[ZoneExitEvent] = self._register_zone_exits_and_get_events(
            dead_vehicle_zone_mappings, event_ts=system_state.cur_frame_ts, truncate=False
        )

        lifetime_events: VehiclesLifetimeEventsContainer = VehiclesLifetimeEventsContainer(
            start=lifetime_start_events, end=lifetime_end_events
        )
        zone_transit_events: ZoneTransitEventsContainer = ZoneTransitEventsContainer(
            start=zone_entrance_events, end=zone_exit_events
        )

        container: CanonicalEventsContainer = CanonicalEventsContainer(lifetime=lifetime_events,
                                                                       zone_transit=zone_transit_events,
                                                                       speeds=speed_update_events)

        return container

    @override
    def _get_events_for_end_of_processing(
            self, *, event_ts: datetime, truncate_events: bool
    ) -> CanonicalEventsContainer:
        dead_vehicle_ids: Set[str] = self._state.vehicle_ids
        dead_vehicle_zone_mappings: Set[VehicleZoneMapping] = self._state.vehicle_zone_mappings
        lifetime_end_events: List[VehicleLifetimeEndEvent] = self._remove_dead_vehicles_and_get_events(
            dead_vehicle_ids, event_ts=event_ts, truncate=truncate_events
        )
        zone_exit_events: List[ZoneExitEvent] = self._register_zone_exits_and_get_events(
            dead_vehicle_zone_mappings, event_ts=event_ts, truncate=truncate_events
        )
        lifetime_events: VehiclesLifetimeEventsContainer = VehiclesLifetimeEventsContainer(
            start=[], end=lifetime_end_events
        )
        zone_transit_events: ZoneTransitEventsContainer = ZoneTransitEventsContainer(
            start=[], end=zone_exit_events
        )
        container: CanonicalEventsContainer = CanonicalEventsContainer(lifetime=lifetime_events,
                                                                       zone_transit=zone_transit_events,
                                                                       speeds=[])
        return container

    @override
    def _clear_own_state(self) -> None:
        self._state.clear()

class CanonicalEventGenerator_Old(BaseFinalEventGenerator[EventsInputData, CanonicalEventsContainer]):
    """
    An object processing `LiveStateInput` for each frame sequentially,
    producing an `EventsContainer` for each.
    Tracks which vehicles and vehicle-zone pairings are alive.
    """

    def __init__(self):
        super().__init__()
        self._prev_state: GeneratorState_Old | None = None

    @override
    @classmethod
    def _get_container_class(cls) -> Type[CanonicalEventsContainer]:
        return CanonicalEventsContainer


    @staticmethod
    def _build_lifetime_end_events(*, dead_vehicle_ids: Set[str],
                                   event_ts: datetime,
                                   truncate: bool) -> List[VehicleLifetimeEndEvent]:
        return [VehicleLifetimeEndEvent(event_id=generate_event_uuid(),
                                        vehicle_id=vehicle_id,
                                        event_ts=event_ts,
                                        truncated=truncate)
                for vehicle_id in dead_vehicle_ids]

    def _build_lifetime_events(self, *, cur_vehicle_ids: Set[str],
                               prev_vehicle_ids: Set[str],
                               cur_vehicle_id_to_type: Dict[str, VehicleType],
                               system_state: ProcessingSystemState) -> VehiclesLifetimeEventsContainer:
        cur_frame_ts: datetime = system_state.cur_frame_ts
        new_vehicle_ids: Set[str] = set.difference(cur_vehicle_ids, prev_vehicle_ids)
        dead_vehicle_ids: Set[str] = set.difference(prev_vehicle_ids, cur_vehicle_ids)
        lifetime_start_events: List[VehicleLifetimeStartEvent] = [
            VehicleLifetimeStartEvent(event_id=generate_event_uuid(),
                                      vehicle_id=vehicle_id,
                                      vehicle_type=cur_vehicle_id_to_type[vehicle_id],
                                      event_ts=cur_frame_ts,
                                      truncated=system_state.is_first_frame)
            for vehicle_id in new_vehicle_ids
        ]
        lifetime_end_events: List[VehicleLifetimeEndEvent] = self._build_lifetime_end_events(
            dead_vehicle_ids=dead_vehicle_ids, event_ts=cur_frame_ts, truncate=False
        )
        container: VehiclesLifetimeEventsContainer = VehiclesLifetimeEventsContainer(
            start=lifetime_start_events, end=lifetime_end_events
        )
        return container

    @staticmethod
    def _build_zone_exit_events(*, dead_mappings: Set[VehicleZoneMapping],
                                event_ts: datetime,
                                truncate: bool) -> List[ZoneExitEvent]:
        return [ZoneExitEvent(event_id=generate_event_uuid(),
                              vehicle_id=vehicle_id,
                              zone_id=zone_id,
                              event_ts=event_ts,
                              truncated=truncate)
                for vehicle_id, zone_id in dead_mappings]

    def _build_zone_occupancy_events(self, *, cur_mappings: Set[VehicleZoneMapping],
                                     prev_mappings: Set[VehicleZoneMapping],
                                     system_state: ProcessingSystemState) -> ZoneTransitEventsContainer:
        cur_frame_ts: datetime = system_state.cur_frame_ts
        new_mappings: Set[VehicleZoneMapping] = set.difference(cur_mappings, prev_mappings)
        dead_mappings: Set[VehicleZoneMapping] = set.difference(prev_mappings, cur_mappings)
        entrance_events: List[ZoneEntranceEvent] = [
            ZoneEntranceEvent(event_id=generate_event_uuid(),
                              vehicle_id=vehicle_id,
                              zone_id=zone_id,
                              event_ts=cur_frame_ts,
                              truncated=system_state.is_first_frame)
            for vehicle_id, zone_id in new_mappings
        ]
        exit_events: List[ZoneExitEvent] = self._build_zone_exit_events(
            dead_mappings=dead_mappings, event_ts=cur_frame_ts, truncate=False
        )
        container: ZoneTransitEventsContainer = ZoneTransitEventsContainer(
            start=entrance_events, end=exit_events
        )
        return container

    def _check_vehicle_type_has_not_changed(self, *, vehicle_id: str, cur_vehicle_type: VehicleType) -> None:
        if self._prev_state is None:
            return
        if vehicle_id in self._prev_state._vehicle_id_to_type:
            prev_vehicle_type: VehicleType = self._prev_state._vehicle_id_to_type[vehicle_id]
            if cur_vehicle_type is not prev_vehicle_type:
                raise ValueError(f"Vehicle type changes are not allowed: "
                                 f"stored {prev_vehicle_type}, got {cur_vehicle_type}")

    @override
    def _update_and_get_events(
            self, input_data: EventsInputData, *, system_state: ProcessingSystemState
    ) -> CanonicalEventsContainer:

        # IDs of vehicles that are alive in the current frame (based on the input)
        vehicle_ids: Set[str] = set()
        # vehicle id -> vehicle type
        vehicle_id_to_type: Dict[str, VehicleType] = dict()
        # (vehicle ID, zone ID) mappings for vehicles
        # that are located in the respective zones (based on the input)
        vehicle_zone_mappings: Set[VehicleZoneMapping] = set()

        speed_update_events: List[SpeedUpdateEvent] = []

        for vehicle in input_data.vehicles:  # type: VehicleInput

            vehicle_id: str = vehicle.vehicle_id
            vehicle_ids.add(vehicle_id)

            cur_vehicle_type: VehicleType = vehicle.vehicle_type
            self._check_vehicle_type_has_not_changed(vehicle_id=vehicle_id,
                                                     cur_vehicle_type=cur_vehicle_type)
            vehicle_id_to_type[vehicle_id] = cur_vehicle_type

            speed_update: SpeedUpdateEvent = SpeedUpdateEvent(
                event_id=generate_event_uuid(),
                vehicle_id=vehicle_id,
                speeds=SpeedsWrapper(raw=vehicle.speeds.raw_ms,
                                     smoothed=vehicle.speeds.smoothed_ms),
                is_matched=vehicle.is_matched,
                event_ts=system_state.cur_frame_ts,
                truncated=system_state.is_first_frame
            )
            speed_update_events.append(speed_update)

            vehicle_zone_mappings.update(
                VehicleZoneMapping(vehicle_id=vehicle_id, zone_id=zone_id)
                for zone_id in vehicle.zone_ids
            )

        lifetime_events: VehiclesLifetimeEventsContainer = self._build_lifetime_events(
            cur_vehicle_ids=vehicle_ids,
            prev_vehicle_ids=self._prev_state._vehicle_ids if self._prev_state is not None else set(),
            cur_vehicle_id_to_type=vehicle_id_to_type,
            system_state=system_state
        )
        zone_occupancy_events: ZoneTransitEventsContainer = self._build_zone_occupancy_events(
            cur_mappings=vehicle_zone_mappings,
            prev_mappings=self._prev_state._vehicle_zone_mappings if self._prev_state is not None else set(),
            system_state=system_state
        )

        container: CanonicalEventsContainer = CanonicalEventsContainer(lifetime=lifetime_events,
                                                                       zone_transit=zone_occupancy_events,
                                                                       speeds=speed_update_events)

        # update the previous state
        new_state: GeneratorState_Old = GeneratorState_Old(
            _vehicle_ids=vehicle_ids,
            _vehicle_id_to_type=vehicle_id_to_type,
            _vehicle_zone_mappings=vehicle_zone_mappings
        )
        self._prev_state = new_state

        return container

    @override
    def _get_events_for_end_of_processing(
            self, *, event_ts: datetime, truncate_events: bool
    ) -> CanonicalEventsContainer:

        speed_update_events: List[SpeedUpdateEvent] = []

        lifetime_end_events: List[VehicleLifetimeEndEvent] = self._build_lifetime_end_events(
            dead_vehicle_ids=self._prev_state._vehicle_ids if self._prev_state is not None else set(),
            event_ts=event_ts,
            truncate=truncate_events
        )
        lifetime_events: VehiclesLifetimeEventsContainer = VehiclesLifetimeEventsContainer(start=[],
                                                                                           end=lifetime_end_events)

        zone_exit_events: List[ZoneExitEvent] = self._build_zone_exit_events(
            dead_mappings=self._prev_state._vehicle_zone_mappings if self._prev_state is not None else set(),
            event_ts=event_ts,
            truncate=truncate_events
        )
        zone_occupancy_events: ZoneTransitEventsContainer = ZoneTransitEventsContainer(
            start=[], end=zone_exit_events
        )

        container: CanonicalEventsContainer = CanonicalEventsContainer(lifetime=lifetime_events,
                                                                       zone_transit=zone_occupancy_events,
                                                                       speeds=speed_update_events)
        return container

    @override
    def _clear_own_state(self) -> None:
        self._prev_state = None