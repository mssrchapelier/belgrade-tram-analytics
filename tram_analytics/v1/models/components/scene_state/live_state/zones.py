from _operator import xor
from datetime import datetime
from typing import List, Self, Literal

from pydantic import BaseModel, PositiveInt, NonNegativeInt, model_validator

from tram_analytics.v1.models.common_types import VehicleType


# --- zones ---

# --- (1) "AT LEAST ONE VEHICLE" periods ---

# Continuous periods during which at least one vehicle, present in the zone, satisfies certain criteria.
# Shared models used for periods with the following definitions:
# (1) at least one vehicle is present inside the zone;
# (2) at least one vehicle is stationary inside the zone.

class BaseExistenceInZonePeriod(BaseModel):
    # The common parameters for any continuous period for a zone
    # defined by the continuous presence of vehicles in it that all satisfy a certain criterion
    # (e. g. any vehicle of the target class; any vehicle that is stationary inside the zone).
    #
    # The fields below are null if the current period is not defined /
    # if no completed period of this type has been registered for the zone yet.

    # The timestamp for the start of this period.
    start_ts: datetime | None
    # The maximum number of vehicles SIMULTANEOUSLY satisfying the defined criteria
    # that was / has been registered at any given time during this period.
    max_vehicles: PositiveInt | None

class CurrentExistenceInZonePeriod(BaseExistenceInZonePeriod):
    # The common parameters for an ongoing period, or for the absence of such.

    # The current number of vehicles in the zone that satisfy the defined criteria.
    num_vehicles: NonNegativeInt
    # The IDs of such vehicles.
    vehicle_ids: List[str]

class CompletedExistenceInZonePeriod(BaseExistenceInZonePeriod):
    # The common parameters for a completed period.

    # The time elapsed since this period has ended
    # (null if no such completed period has been registered yet).
    end_ts: datetime | None

class PreviousAndCurrentExistenceInZonePeriodContainer(BaseModel):
    current_period: CurrentExistenceInZonePeriod
    last_completed_period: CompletedExistenceInZonePeriod

# --- (2) "ONE VEHICLE LAST DID THIS ONE TIME" periods ---

# The last completed continuous period during which ANY ONE SINGLE vehicle satisfied certain criteria.
# Shared models used for periods with the following definitions:
# (1) the last period during which any one single vehicle was stationary inside the zone
#     (i. e. a tram stopping at the platform:
#     the duration of and time elapsed from the end of last such stop)
# (2) "pass": the last period during which any one single vehicle was present inside the zone
#     (i. e. a tram passing through a specified zone:
#     the duration of last such pass and the time elapsed since it ended)

class LastCompletedByVehiclePeriod(BaseModel):
    # The fields below are null if no completed period of this kind
    # has been registered for the zone yet.

    # The duration of the period for which the vehicle satisfied the criteria.
    #
    # Rationale for storing an ARRAY:
    # Cases are possible in which more than one vehicle stops satisfying the criteria simultaneously.
    # In that case, the respective durations of their individual periods are stored.
    #
    # Example:
    # More than one tram departing from the platform at the same moment.
    # In that case, `ended_ago_s` is the same, but there are multiple values
    # for `duration_s` which are all passed.
    start_timestamps: List[datetime] | None
    # The time elapsed since the moment when the vehicle stopped satisfying the criteria.
    end_ts: datetime | None

    @model_validator(mode="after")
    def _validate_model(self) -> Self:
        # ensure that:
        # (1) either both start_timestamps and end_ts are null
        # (2) or that: (2.1) both are not null and (2.2) start_timestamps is not empty

        # (1) and (2.1): through xor
        if xor(self.start_timestamps is None, self.end_ts is None):
            raise ValueError("start_timestamps and end_ts must be both null or both not null")
        if self.end_ts is not None:
            # assertion backed by xnor above; including here for mypy inference
            assert self.start_timestamps is not None
            # ensure (2.2) given (2)
            if len(self.start_timestamps) == 0:
                raise ValueError("start_timestamps must be a non-empty list or null if end_ts is not null")
        # otherwise: ok
        return self

# --- containers for zone periods ---

class PeriodsForZone(BaseModel):
    by_at_least_one_vehicle: PreviousAndCurrentExistenceInZonePeriodContainer
    last_completed_by_vehicle: LastCompletedByVehiclePeriod

# --- zone metadata ---

class BaseZoneMetadata(BaseModel):
    # the unique ID for this zone
    zone_id: str
    # the camera-specific, zone type-specific numerical ID for ease of readability
    # (e. g.: track 2, platform 1, intrusion zone 1)
    zone_numerical_id: int
    # a text description of the zone
    description: str

class AgnosticZoneMetadata(BaseZoneMetadata):
    pass

class BaseTramZoneMetadata(BaseZoneMetadata):
    pass

class TrackMetadata(BaseTramZoneMetadata):
    pass

class PlatformMetadata(BaseTramZoneMetadata):
    # the ID of the track to which this platform belongs
    track_zone_id: str

class BaseCarZoneMetadata(BaseZoneMetadata):
    pass

class IntrusionZoneMetadata(BaseCarZoneMetadata):
    pass

# --- master zone state object ---

class BaseZone[Metadata: BaseZoneMetadata](BaseModel):

    metadata: Metadata

    # NOTE: Everything below applies only to the vehicles belonging to the target class
    # (i. e., when speaking of e. g. the current number of vehicles in the zone,
    # for tram zones, only trams are being taken into account).

    # Data related to vehicles being present in the zone:
    # (1) occupancy periods (when at least one vehicle is present in the zone) -- current and last completed;
    # (2) the last pass completed by a vehicle.
    # Useful for tracking the use of the zone generally.
    occupancy: PeriodsForZone

    # Data related to vehicles being stationary in the zone:
    # (1) the current and the last completed periods during which
    #     at least one vehicle was stationary whilst inside the zone;
    # (2) the last stop completed by a vehicle.
    # Useful for tracking stops of public transport
    # and of traffic congestion inside the zone.
    stops: PeriodsForZone

class AgnosticZone(BaseZone[AgnosticZoneMetadata]):
    pass

# --- tram zones (track and platform zones) ---

class BaseTramZone[Metadata: BaseTramZoneMetadata](BaseZone[Metadata]):
    target_vehicle_type: Literal[VehicleType.TRAM] = VehicleType.TRAM

class BaseCarZone[Metadata: BaseCarZoneMetadata](BaseZone[Metadata]):
    target_vehicle_type: Literal[VehicleType.CAR] = VehicleType.CAR

class Track(BaseTramZone[TrackMetadata]):
    pass

class Platform(BaseTramZone[PlatformMetadata]):
    pass

class IntrusionZone(BaseCarZone[IntrusionZoneMetadata]):
    pass

class BaseZoneGroupsByVehicleTypeContainer(BaseModel):
    pass

class ZoneGroupsForTramContainer(BaseZoneGroupsByVehicleTypeContainer):
    tracks: List[Track]
    platforms: List[Platform]

class ZoneGroupsForCarContainer(BaseZoneGroupsByVehicleTypeContainer):
    intrusion_zones: List[IntrusionZone]

class BaseZonesContainer(BaseModel):
    pass

class AgnosticZonesContainer(BaseZonesContainer):
    all_zones: List[AgnosticZone]

class ZonesContainer(BaseZonesContainer):
    tram_zones: ZoneGroupsForTramContainer
    car_zones: ZoneGroupsForCarContainer
