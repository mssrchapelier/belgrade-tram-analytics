from datetime import datetime
from typing import List, Dict, override, Type, Set, Iterable

from tram_analytics.v1.models.components.scene_state.events.base import generate_event_uuid, generate_period_uuid
from tram_analytics.v1.models.components.scene_state.events.subprocessors.zone_transit import (
    ZoneEntranceEvent, ZoneExitEvent, ZoneTransitEventsContainer
)
from tram_analytics.v1.pipeline.components.scene_state.events.pipeline.events_input import EventsInputData
from tram_analytics.v1.pipeline.components.scene_state.events.subprocessors.base.base_generator import (
    BaseFinalEventGenerator, BaseGeneratorState, ProcessingSystemState
)
from tram_analytics.v1.pipeline.components.scene_state.events.subprocessors.base.base_types import \
    VehicleZoneMapping


class ZoneTransitGeneratorState(BaseGeneratorState):

    def __init__(self) -> None:
        super().__init__()
        # (vehicle ID, zone ID) -> zone occupancy period ID
        self._vehicle_zone_mapping_to_transit_period_id: Dict[VehicleZoneMapping, str] = dict()

    def get_vehicle_zone_mappings(self) -> Set[VehicleZoneMapping]:
        return set(self._vehicle_zone_mapping_to_transit_period_id.keys())

    def add_vehicle_zone_mapping(self, *, mapping: VehicleZoneMapping, transit_period_id: str) -> None:
        if mapping in self._vehicle_zone_mapping_to_transit_period_id:
            raise ValueError(f"Can't register mapping (vehicle {mapping.vehicle_id}, "
                             f"zone {mapping.zone_id}): already registered")
        self._vehicle_zone_mapping_to_transit_period_id[mapping] = transit_period_id

    def remove_vehicle_zone_mapping_and_get_transit_period_id(self, mapping: VehicleZoneMapping) -> str:
        if mapping not in self._vehicle_zone_mapping_to_transit_period_id:
            raise ValueError(f"Can't deregister mapping (vehicle {mapping.vehicle_id}, "
                             f"zone {mapping.zone_id}): not registered")
        return self._vehicle_zone_mapping_to_transit_period_id.pop(mapping)

    @override
    def _clear_own_state(self) -> None:
        self._vehicle_zone_mapping_to_transit_period_id.clear()

class ZoneTransitEventGenerator(
    BaseFinalEventGenerator[EventsInputData, ZoneTransitEventsContainer, ZoneTransitGeneratorState]
):

    @override
    @classmethod
    def _get_container_class(cls) -> Type[ZoneTransitEventsContainer]:
        return ZoneTransitEventsContainer

    @override
    @classmethod
    def _get_new_state(cls) -> ZoneTransitGeneratorState:
        return ZoneTransitGeneratorState()

    def register_entrances_and_get_events(
            self, new_mappings: Iterable[VehicleZoneMapping], *, system_state: ProcessingSystemState
    ) -> List[ZoneEntranceEvent]:
        events: List[ZoneEntranceEvent] = []
        for mapping in new_mappings: # type: VehicleZoneMapping
            transit_period_id: str = generate_period_uuid()
            self._state.add_vehicle_zone_mapping(mapping=mapping,
                                                 transit_period_id=transit_period_id)
            event: ZoneEntranceEvent = ZoneEntranceEvent(
                camera_id=self._camera_id,
                event_id=generate_event_uuid(),
                period_id=transit_period_id,
                vehicle_id=mapping.vehicle_id,
                zone_id=mapping.zone_id,
                event_ts=system_state.cur_frame_ts,
                truncated=system_state.is_first_frame
            )
            events.append(event)
        return events

    def register_exits_and_get_events(
            self, dead_mappings: Iterable[VehicleZoneMapping], *, event_ts: datetime, truncate: bool
    ) -> List[ZoneExitEvent]:
        events: List[ZoneExitEvent] = []
        for mapping in dead_mappings:  # type: VehicleZoneMapping
            transit_period_id: str = self._state.remove_vehicle_zone_mapping_and_get_transit_period_id(
                mapping
            )
            event: ZoneExitEvent = ZoneExitEvent(
                camera_id=self._camera_id,
                event_id=generate_event_uuid(),
                period_id=transit_period_id,
                vehicle_id=mapping.vehicle_id,
                zone_id=mapping.zone_id,
                event_ts=event_ts,
                truncated=truncate
            )
            events.append(event)
        return events

    @override
    def _update_and_get_events(
            self, input_data: EventsInputData, *, system_state: ProcessingSystemState
    ) -> ZoneTransitEventsContainer:
        # mapping: (vehicle_id, zone_id)
        prev_mappings: Set[VehicleZoneMapping] = self._state.get_vehicle_zone_mappings()
        mappings_in_input: Set[VehicleZoneMapping] = {
            VehicleZoneMapping(vehicle_id=vehicle.vehicle_id,
                               zone_id=zone_id)
            for vehicle in input_data.vehicles
            for zone_id in vehicle.zone_ids
        }
        new_mappings: Set[VehicleZoneMapping] = set.difference(mappings_in_input, prev_mappings)
        dead_mappings: Set[VehicleZoneMapping] = set.difference(prev_mappings, mappings_in_input)

        entrance_events: List[ZoneEntranceEvent] = self.register_entrances_and_get_events(
            new_mappings, system_state=system_state
        )
        exit_events: List[ZoneExitEvent] = self.register_exits_and_get_events(
            dead_mappings, event_ts=system_state.cur_frame_ts, truncate=system_state.is_first_frame
        )
        container: ZoneTransitEventsContainer = ZoneTransitEventsContainer(
            start=entrance_events, end=exit_events
        )
        return container

    @override
    def _get_events_for_end_of_processing(
            self, *, event_ts: datetime, truncate_events: bool
    ) -> ZoneTransitEventsContainer:
        dead_mappings: Set[VehicleZoneMapping] = self._state.get_vehicle_zone_mappings()
        exit_events: List[ZoneExitEvent] = self.register_exits_and_get_events(
            dead_mappings, event_ts=event_ts, truncate=truncate_events
        )
        container: ZoneTransitEventsContainer = ZoneTransitEventsContainer(
            start=[], end=exit_events
        )
        return container