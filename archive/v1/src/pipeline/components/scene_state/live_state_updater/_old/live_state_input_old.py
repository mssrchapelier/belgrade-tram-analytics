from typing import List, Annotated, Set, TypeAlias, Self, Literal

from pydantic import BaseModel, Field, NonNegativeFloat, model_validator

from tram_analytics.v1.models.common_types import VehicleType

class SpeedsInput(BaseModel):
    # RESTRICTION:
    # For every `n`, for every `speed_type` in `{ raw_kmh, smoothed_kmh }`,
    # if `speed_type` was a `float` at frame `n`,
    # then it must also be a `float` at frame `n+1`.
    # In other words, once the speed value has been set to a numerical value,
    # it cannot be set to None for subsequent frames.

    raw_kmh: NonNegativeFloat | None
    smoothed_kmh: NonNegativeFloat | None

class BaseVehicleInput(BaseModel):
    vehicle_id: str
    is_presence_confirmed: bool
    speeds: SpeedsInput

class TramInput(BaseVehicleInput):
    vehicle_type: Literal[VehicleType.TRAM] = VehicleType.TRAM
    track_ids: Set[str]
    platform_ids: Set[str]

class CarInput(BaseVehicleInput):
    vehicle_type: Literal[VehicleType.CAR] = VehicleType.CAR
    intrusion_zone_ids: Set[str]

VehicleInput: TypeAlias = Annotated[TramInput | CarInput, Field(discriminator="vehicle_type")]

class VehicleInputWrapper(BaseModel):
    trams: List[TramInput]
    cars: List[CarInput]

    def _check_unique_vehicle_ids(self) -> None:
        tram_ids: Set[str] = {tram.vehicle_id for tram in self.trams}
        car_ids: Set[str] = {car.vehicle_id for car in self.cars}
        all_ids: Set[str] = set.union(tram_ids, car_ids)
        total_objects: int = len(self.trams) + len(self.cars)
        if not len(all_ids) == total_objects:
            raise ValueError("Duplicate vehicle IDs found")

    @model_validator(mode="after")
    def _validate_vehicle_ids(self) -> Self:
        # all vehicle IDs must be unique
        self._check_unique_vehicle_ids()
        return self

# --- master object ---

class LiveStateInput(BaseModel):
    camera_id: str

    frame_id: str
    frame_ts: NonNegativeFloat

    vehicles: VehicleInputWrapper