from datetime import datetime
from typing import List, Set, Self

from pydantic import BaseModel, NonNegativeFloat, model_validator

from tram_analytics.v1.models.common_types import VehicleType


class SpeedsInput(BaseModel):
    # RESTRICTION:
    # For every `n`, for every `speed_type` in `{ raw_kmh, smoothed_kmh }`,
    # if `speed_type` was a `float` at frame `n`,
    # then it must also be a `float` at frame `n+1`.
    # In other words, once the speed value has been set to a numerical value,
    # it cannot be set to None for subsequent frames.

    raw_ms: NonNegativeFloat | None
    smoothed_ms: NonNegativeFloat | None

class VehicleInput(BaseModel):
    vehicle_type: VehicleType
    vehicle_id: str
    is_matched: bool
    speeds: SpeedsInput
    zone_ids: Set[str]

# --- master object ---

class EventsInputData(BaseModel):

    camera_id: str

    frame_id: str
    frame_ts: datetime

    vehicles: List[VehicleInput]

    def _check_unique_vehicle_ids(self) -> None:
        vehicle_ids: Set[str] = {vehicle.vehicle_id for vehicle in self.vehicles}
        num_unique_ids: int = len(vehicle_ids)
        num_vehicles: int = len(self.vehicles)
        if not num_unique_ids == num_vehicles:
            raise ValueError("Duplicate vehicle IDs found")

    @model_validator(mode="after")
    def _validate_vehicle_ids(self) -> Self:
        # all vehicle IDs must be unique
        self._check_unique_vehicle_ids()
        return self