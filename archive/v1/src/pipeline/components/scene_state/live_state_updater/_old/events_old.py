from typing import List, Literal, TypeAlias, Self, Set, Tuple, Dict
from enum import Enum
from dataclasses import dataclass

from pydantic import BaseModel, NonNegativeFloat, model_validator

from tram_analytics.v1.models.common_types import VehicleType
from tram_analytics.v1.pipeline.components.scene_state.events.pipeline.events_input import EventsInputData, VehicleInputWrapper, VehicleInput

class EventBoundaryType(str, Enum):
    START = "start"
    END = "end"

class BaseEvent(BaseModel):
    vehicle_id: str

class BoundaryEvent(BaseEvent):
    boundary_type: EventBoundaryType

class LifetimeStartEvent(BoundaryEvent):
    boundary_type: Literal[EventBoundaryType.START] = EventBoundaryType.START
    vehicle_type: VehicleType

class LifetimeEndEvent(BoundaryEvent):
    boundary_type: Literal[EventBoundaryType.END] = EventBoundaryType.END

LifetimeEvent: TypeAlias = LifetimeStartEvent | LifetimeEndEvent

class ZoneOccupancyEvent(BoundaryEvent):
    zone_id: str

class SpeedsWrapper(BaseModel):
    raw: NonNegativeFloat | None
    smoothed: NonNegativeFloat | None

class SpeedUpdateEvent(BaseEvent):
    speeds: SpeedsWrapper

class EventsWrapper(BaseModel):
    """
    Holds all events for a single frame.
    """

    lifetime: List[LifetimeEvent]
    zone_occupancy: List[ZoneOccupancyEvent]
    speeds: List[SpeedUpdateEvent]

    # --- model validation functions ---

    def _check_lifetime_vehicle_ids_unique(self) -> None:
        lifetime_vehicle_ids: Set[str] = {e.vehicle_id for e in self.lifetime}
        if not len(lifetime_vehicle_ids) == len(self.lifetime):
            raise ValueError("Vehicle IDs in lifetime events must be unique")

    def _check_speeds_vehicle_ids_unique(self) -> None:
        speeds_vehicle_ids: Set[str] = {e.vehicle_id for e in self.speeds}
        if not len(speeds_vehicle_ids) == len(self.speeds):
            raise ValueError("Vehicle IDs in speeds events must be unique")

    def _check_zone_events_ids(self) -> None:
        # (vehicle id, zone id) tuples must be unique

        # (vehicle id, zone id)
        event_keys: Set[Tuple[str, str]] = {
            (e.vehicle_id, e.zone_id)
            for e in self.zone_occupancy
        }
        if len(event_keys) != len(self.zone_occupancy):
            raise ValueError("Only one zone occupancy event can be defined "
                             "for any combination of vehicle ID and zone ID")

    def _check_ids_existence_in_speeds(self) -> None:
        lifetime_vehicle_ids: Set[str] = {e.vehicle_id for e in self.lifetime}
        zone_occupancy_vehicle_ids: Set[str] = {e.vehicle_id for e in self.zone_occupancy}
        speeds_vehicle_ids: Set[str] = {e.vehicle_id for e in self.speeds}

        if len(set.intersection(lifetime_vehicle_ids, speeds_vehicle_ids)) > 0:
            raise ValueError("All vehicle IDs from lifetime must appear in speeds")
        if len(set.intersection(zone_occupancy_vehicle_ids, speeds_vehicle_ids)) > 0:
            raise ValueError("All vehicle IDs from zone occupancy must appear in speeds")

    @model_validator(mode="after")
    def _validate_model(self) -> Self:
        # (1) all vehicle IDs from lifetime and zone occupancy must appear in speeds
        # (speeds must be defined for all vehicles that are alive)
        self._check_ids_existence_in_speeds()
        # (2) lifetime: vehicle ids must be unique
        self._check_lifetime_vehicle_ids_unique()
        # (3) zone occupancy: (vehicle id, zone id) tuples must be unique
        self._check_zone_events_ids()
        # (4) speeds: vehicle ids must be unique
        self._check_speeds_vehicle_ids_unique()
        return self


class EventsContainer(BaseModel):

    camera_id: str

    frame_id: str
    frame_ts: NonNegativeFloat

    events: EventsWrapper

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
    frame_ts: float

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
            prev_ts: float = self._prev_state.frame_ts
            cur_ts: float = input_obj.frame_ts
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

        vehicles_input: VehicleInputWrapper = input_obj.vehicles

        # IDs of vehicles that are alive in the current frame (based on the input)
        vehicle_ids: Set[str] = set()
        # vehicle id -> vehicle type
        vehicle_id_to_type: Dict[str, VehicleType] = dict()
        # (vehicle ID, zone ID) mappings for vehicles
        # that are located in the respective zones (based on the input)
        vehicle_zone_mappings: Set[VehicleZoneMapping] = set()

        speed_update_events: List[SpeedUpdateEvent] = []

        for vehicle in [*vehicles_input.trams, *vehicles_input.cars]:  # type: VehicleInput

            vehicle_id: str = vehicle.vehicle_id
            vehicle_ids.add(vehicle_id)
            vehicle_id_to_type[vehicle_id] = vehicle.vehicle_type

            speed_update: SpeedUpdateEvent = SpeedUpdateEvent(
                vehicle_id=vehicle_id,
                speeds=SpeedsWrapper(raw=vehicle.speeds.raw_ms,
                                     smoothed=vehicle.speeds.smoothed_ms)
            )
            speed_update_events.append(speed_update)

            zone_ids: Set[str] = (
                vehicle.intrusion_zone_ids if vehicle.vehicle_type == VehicleType.CAR
                else set.union(vehicle.track_ids, vehicle.platform_ids)
            )
            vehicle_zone_mappings.update(
                (vehicle_id, zone_id) for zone_id in zone_ids
            )

        lifetime_events: List[LifetimeEvent] = self._build_lifetime_events(
            cur_vehicle_ids=vehicle_ids,
            prev_vehicle_ids=self._prev_state.vehicle_ids,
            cur_vehicle_id_to_type=vehicle_id_to_type
        )
        zone_occupancy_events: List[ZoneOccupancyEvent] = self._build_zone_occupancy_events(
            cur_mappings=vehicle_zone_mappings,
            prev_mappings=self._prev_state.vehicle_zone_mappings
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