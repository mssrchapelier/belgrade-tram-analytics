from datetime import datetime
from typing import List, Dict, Set, override, Type, Iterable

from tram_analytics.v1.models.components.scene_state.events.base import generate_event_uuid, generate_period_uuid
from tram_analytics.v1.models.components.scene_state.events.subprocessors.lifetime import (
    VehicleLifetimeStartEvent, VehicleLifetimeEndEvent, VehiclesLifetimeEventsContainer
)
from tram_analytics.v1.pipeline.components.scene_state.events.pipeline.events_input import EventsInputData, \
    VehicleInput
from tram_analytics.v1.pipeline.components.scene_state.events.subprocessors.base.base_generator import (
    BaseFinalEventGenerator, ProcessingSystemState
)
from tram_analytics.v1.pipeline.components.scene_state.events.subprocessors.base.base_generator import \
    BaseGeneratorState
from tram_analytics.v1.pipeline.components.scene_state.events.subprocessors.base.base_types import \
    VehicleIdTypeMapping


class LifetimeEventGeneratorState(BaseGeneratorState):

    def __init__(self) -> None:
        super().__init__()
        # vehicles that are currently alive
        self._vehicle_id_to_lifetime_period_id: Dict[str, str] = dict()

    def get_vehicle_ids(self):
        return set(self._vehicle_id_to_lifetime_period_id.keys())

    def add_vehicle(self, *, vehicle_id: str, lifetime_period_id: str) -> None:
        if vehicle_id in self._vehicle_id_to_lifetime_period_id:
            raise ValueError(f"Can't begin lifetime for vehicle {vehicle_id}: already registered")
        self._vehicle_id_to_lifetime_period_id[vehicle_id] = lifetime_period_id

    def remove_vehicle_and_get_lifetime_period_id(self, vehicle_id: str) -> str:
        if vehicle_id not in self._vehicle_id_to_lifetime_period_id:
            raise ValueError(f"Can't end lifetime for vehicle {vehicle_id}: not registered")
        return self._vehicle_id_to_lifetime_period_id.pop(vehicle_id)

    @override
    def _clear_own_state(self) -> None:
        self._vehicle_id_to_lifetime_period_id.clear()

class LifetimeEventGenerator(
    BaseFinalEventGenerator[EventsInputData, VehiclesLifetimeEventsContainer, LifetimeEventGeneratorState]
):

    @override
    @classmethod
    def _get_new_state(cls) -> LifetimeEventGeneratorState:
        return LifetimeEventGeneratorState()

    @override
    @classmethod
    def _get_container_class(cls) -> Type[VehiclesLifetimeEventsContainer]:
        return VehiclesLifetimeEventsContainer

    def _begin_lifetimes_and_get_events(
            self, mappings: Iterable[VehicleIdTypeMapping], *, system_state: ProcessingSystemState
    ) -> List[VehicleLifetimeStartEvent]:
        events: List[VehicleLifetimeStartEvent] = []
        for mapping in mappings:  # type: VehicleIdTypeMapping
            vehicle_id: str = mapping.vehicle_id
            lifetime_period_id: str = generate_period_uuid()
            self._state.add_vehicle(vehicle_id=vehicle_id,
                                    lifetime_period_id=lifetime_period_id)
            event: VehicleLifetimeStartEvent = VehicleLifetimeStartEvent(
                camera_id=self._camera_id,
                event_id=generate_event_uuid(),
                period_id=lifetime_period_id,
                vehicle_id=vehicle_id,
                vehicle_type=mapping.vehicle_type,
                event_ts=system_state.cur_frame_ts,
                truncated=system_state.is_first_frame
            )
            events.append(event)
        return events

    def _end_lifetimes_and_get_events(
            self, dead_vehicle_ids: Iterable[str], *, event_ts: datetime, truncate: bool
    ) -> List[VehicleLifetimeEndEvent]:
        events: List[VehicleLifetimeEndEvent] = []
        for vehicle_id in dead_vehicle_ids:  # type: str
            lifetime_period_id: str = self._state.remove_vehicle_and_get_lifetime_period_id(vehicle_id)
            event: VehicleLifetimeEndEvent = VehicleLifetimeEndEvent(
                camera_id=self._camera_id,
                event_id=generate_event_uuid(),
                period_id=lifetime_period_id,
                vehicle_id=vehicle_id,
                event_ts=event_ts,
                truncated=truncate
            )
            events.append(event)
        return events

    @override
    def _update_and_get_events(
            self, input_data: EventsInputData, *, system_state: ProcessingSystemState
    ) -> VehiclesLifetimeEventsContainer:
        prev_vehicle_ids: Set[str] = self._state.get_vehicle_ids()
        vehicle_ids_in_input: Set[str] = {vehicle.vehicle_id for vehicle in input_data.vehicles}
        new_vehicle_ids: Set[str] = set.difference(vehicle_ids_in_input, prev_vehicle_ids)
        dead_vehicle_ids: Set[str] = set.difference(prev_vehicle_ids, vehicle_ids_in_input)

        # vehicle id -> VehicleInput
        vehicle_id_to_input: Dict[str, VehicleInput] = {
            vehicle_input.vehicle_id: vehicle_input
            for vehicle_input in input_data.vehicles
        }
        mappings_for_start_events: List[VehicleIdTypeMapping] = [
            VehicleIdTypeMapping(vehicle_id=v_id,
                                 vehicle_type=vehicle_id_to_input[v_id].vehicle_type)
            for v_id in new_vehicle_ids
        ]

        lifetime_start_events: List[VehicleLifetimeStartEvent] = self._begin_lifetimes_and_get_events(
            mappings_for_start_events, system_state=system_state
        )
        lifetime_end_events: List[VehicleLifetimeEndEvent] = self._end_lifetimes_and_get_events(
            dead_vehicle_ids, event_ts=system_state.cur_frame_ts, truncate=system_state.is_first_frame
        )

        container: VehiclesLifetimeEventsContainer = VehiclesLifetimeEventsContainer(
            start=lifetime_start_events, end=lifetime_end_events
        )
        return container

    @override
    def _get_events_for_end_of_processing(
            self, *, event_ts: datetime, truncate_events: bool
    ) -> VehiclesLifetimeEventsContainer:
        dead_vehicle_ids: Set[str] = self._state.get_vehicle_ids()
        lifetime_end_events: List[VehicleLifetimeEndEvent] = self._end_lifetimes_and_get_events(
            dead_vehicle_ids, event_ts=event_ts, truncate=truncate_events
        )
        container: VehiclesLifetimeEventsContainer = VehiclesLifetimeEventsContainer(
            start=[], end=lifetime_end_events
        )
        return container
