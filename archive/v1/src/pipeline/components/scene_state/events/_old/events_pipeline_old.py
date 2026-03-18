from typing import List, TypeAlias, Set, Tuple, Dict
from dataclasses import dataclass
from datetime import datetime

from tram_analytics.v1.models.common_types import VehicleType
from tram_analytics.v1.pipeline.components.scene_state.events.pipeline.events_input import EventsInputData, VehicleInput
from archive.v1.src.pipeline.components.scene_state.events._old.events_old import (
    EventBoundaryType, LifetimeStartEvent, LifetimeEndEvent, LifetimeEvent,
    ZoneOccupancyEvent, SpeedsWrapper, SpeedUpdateEvent, EventsWrapper, EventsContainer
)

# (vehicle id, zone id): represents a mapping between a vehicle
# and a zone in which it is currently located
VehicleZoneMapping: TypeAlias = Tuple[str, str]

@dataclass(frozen=True, slots=True)
class GeneratorState:
    # vehicle IDs of vehicles that are alive
    vehicle_ids: Set[str]
    # vehicle id -> vehicle type
    vehicle_id_to_type: Dict[str, VehicleType]
    # (vehicle ID, zone ID) tuples for vehicles that are currently in a specific zone
    vehicle_zone_mappings: Set[VehicleZoneMapping]

    camera_id: str
    frame_ts: datetime

class EventsContainerGenerator:
    """
    An object processing `LiveStateInput` for each frame sequentially,
    producing an `EventsContainer` for each.
    Tracks which vehicles and vehicle-zone pairings are alive.
    """

    def __init__(self):
        self._prev_state: GeneratorState | None = None

    def _check_frame_input(self, input_obj: EventsInputData) -> None:
        if self._prev_state:
            # check the camera ID
            prev_cam_id: str = self._prev_state.camera_id
            cur_cam_id: str = input_obj.camera_id
            if cur_cam_id != prev_cam_id:
                raise ValueError("Passed a different camera ID than the one received previously "
                                 f"(expected {prev_cam_id}, got {cur_cam_id})")
            # check the frame timestamp: must be greater than the previous one
            prev_ts: datetime = self._prev_state.frame_ts
            cur_ts: datetime = input_obj.frame_ts
            if not cur_ts > prev_ts:
                raise ValueError("The current frame timestamp must be greater than the previous one "
                                 f"(previous {prev_ts}, received current: {cur_ts})")

    @staticmethod
    def _build_lifetime_events(*, cur_vehicle_ids: Set[str],
                               prev_vehicle_ids: Set[str],
                               cur_vehicle_id_to_type: Dict[str, VehicleType]) -> List[LifetimeEvent]:
        new_vehicle_ids: Set[str] = set.difference(cur_vehicle_ids, prev_vehicle_ids)
        dead_vehicle_ids: Set[str] = set.difference(prev_vehicle_ids, cur_vehicle_ids)
        lifetime_start_events: List[LifetimeStartEvent] = [
            LifetimeStartEvent(vehicle_id=vehicle_id,
                               vehicle_type=cur_vehicle_id_to_type[vehicle_id])
            for vehicle_id in new_vehicle_ids
        ]
        lifetime_end_events: List[LifetimeEndEvent] = [
            LifetimeEndEvent(vehicle_id=vehicle_id)
            for vehicle_id in dead_vehicle_ids
        ]
        events: List[LifetimeEvent] = [*lifetime_start_events, *lifetime_end_events]
        return events

    @staticmethod
    def _build_zone_occupancy_events(*, cur_mappings: Set[VehicleZoneMapping],
                                     prev_mappings: Set[VehicleZoneMapping]) -> List[ZoneOccupancyEvent]:
        new_mappings: Set[VehicleZoneMapping] = set.difference(cur_mappings, prev_mappings)
        dead_mappings: Set[VehicleZoneMapping] = set.difference(prev_mappings, cur_mappings)
        entrance_events: List[ZoneOccupancyEvent] = [
            ZoneOccupancyEvent(vehicle_id=vehicle_id, zone_id=zone_id,
                               boundary_type=EventBoundaryType.START)
            for vehicle_id, zone_id in new_mappings
        ]
        exit_events: List[ZoneOccupancyEvent] = [
            ZoneOccupancyEvent(vehicle_id=vehicle_id, zone_id=zone_id,
                               boundary_type=EventBoundaryType.END)
            for vehicle_id, zone_id in dead_mappings
        ]
        events: List[ZoneOccupancyEvent] = [*entrance_events, *exit_events]
        return events

    def update_and_get_events(self, input_obj: EventsInputData) -> EventsContainer:

        # IDs of vehicles that are alive in the current frame (based on the input)
        vehicle_ids: Set[str] = set()
        # vehicle id -> vehicle type
        vehicle_id_to_type: Dict[str, VehicleType] = dict()
        # (vehicle ID, zone ID) mappings for vehicles
        # that are located in the respective zones (based on the input)
        vehicle_zone_mappings: Set[VehicleZoneMapping] = set()

        speed_update_events: List[SpeedUpdateEvent] = []

        for vehicle in input_obj.vehicles:  # type: VehicleInput

            vehicle_id: str = vehicle.vehicle_id
            vehicle_ids.add(vehicle_id)
            vehicle_id_to_type[vehicle_id] = vehicle.vehicle_type

            speed_update: SpeedUpdateEvent = SpeedUpdateEvent(
                vehicle_id=vehicle_id,
                speeds=SpeedsWrapper(raw=vehicle.speeds.raw_ms,
                                     smoothed=vehicle.speeds.smoothed_ms),
                is_matched=vehicle.is_matched
            )
            speed_update_events.append(speed_update)

            vehicle_zone_mappings.update(
                (vehicle_id, zone_id) for zone_id in vehicle.zone_ids
            )

        lifetime_events: List[LifetimeEvent] = self._build_lifetime_events(
            cur_vehicle_ids=vehicle_ids,
            prev_vehicle_ids=self._prev_state.vehicle_ids if self._prev_state is not None else set(),
            cur_vehicle_id_to_type=vehicle_id_to_type
        )
        zone_occupancy_events: List[ZoneOccupancyEvent] = self._build_zone_occupancy_events(
            cur_mappings=vehicle_zone_mappings,
            prev_mappings=self._prev_state.vehicle_zone_mappings if self._prev_state is not None else set()
        )

        events_wrapper: EventsWrapper = EventsWrapper(lifetime=lifetime_events,
                                                      zone_occupancy=zone_occupancy_events,
                                                      speeds=speed_update_events)
        container: EventsContainer = EventsContainer(camera_id=input_obj.camera_id,
                                                     frame_id=input_obj.frame_id,
                                                     frame_ts=input_obj.frame_ts,
                                                     events=events_wrapper)

        # update the previous state
        new_state: GeneratorState = GeneratorState(
            camera_id=input_obj.camera_id,
            frame_ts=input_obj.frame_ts,
            vehicle_ids=vehicle_ids,
            vehicle_id_to_type=vehicle_id_to_type,
            vehicle_zone_mappings=vehicle_zone_mappings
        )
        self._prev_state = new_state

        return container