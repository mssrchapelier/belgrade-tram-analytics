from typing import Literal, TypeAlias, List, Set, Tuple, Self
from enum import Enum
from datetime import datetime

from pydantic import BaseModel, NonNegativeFloat, model_validator

from tram_analytics.v1.models.common_types import VehicleType

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
    is_matched: bool

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
    frame_ts: datetime

    events: EventsWrapper
