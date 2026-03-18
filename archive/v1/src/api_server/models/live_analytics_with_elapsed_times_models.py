from typing import List, Literal
from datetime import datetime

from pydantic import BaseModel, NonNegativeFloat, NonNegativeInt, PositiveInt

from tram_analytics.v1.pipeline.components.scene_state.config.scene_events_config import SceneEventsConfig
from tram_analytics.v1.models.common_types import VehicleType

# --- DESCRIPTION ---
# The older version of the API payload for live state that listed elapsed times.
# Changed to include the actual timestamp; the client will compute the elapsed times on their own.

# NOTE:
# "Agnostic" classes below define output that treats all vehicle types
# as being the same and all zone types likewise.
#
# In regular output, in contrast:
# (1) zones are split into tram zones (and there further into tracks and platforms)
#     and car zones (intrusion zones), with platforms being the only one containing
#     the zone ID and numerical ID of the track to which they belong;
# (2) vehicles are split into trams and cars (with in-zone information for trams
#     being split into tracks and platforms, and that for cars, wrapped into an intrusion zone container).
#
# This does add conversion overhead between the two types, but is useful for future extension of this schema
# with new zone types, allowing the vehicle/zone sub-processors to stay agnostic of the vehicle's/zone's actual type,
# the relevant information only being added in the main processor which stores the corresponding mappings.


# --- request payload ---

# class APILiveRequestPayload(BaseModel):
#     pass

# --- speeds ---

class SpeedStats(BaseModel):
    # aggregated statistics wrt the vehicle's lifetime
    max_kmh: NonNegativeFloat | None
    mean_kmh: NonNegativeFloat | None
    median_kmh: NonNegativeFloat | None

class SpeedStatsWithCurrent(SpeedStats):
    current_kmh: NonNegativeFloat | None

class BaseSpeedWrapper[SpeedContainer: SpeedStats](BaseModel):
    raw: SpeedContainer
    smoothed: SpeedContainer

class LifetimeSpeeds(BaseSpeedWrapper[SpeedStatsWithCurrent]):
    # includes the current speed
    pass

class InZoneSpeeds(BaseSpeedWrapper[SpeedStats]):
    # does not include the current speed (to avoid duplication)
    pass

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

    # The duration of this period.
    duration_s: NonNegativeFloat | None
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
    ended_ago_s: NonNegativeFloat | None

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
    duration_s: List[NonNegativeFloat] | None
    # The time elapsed since the moment when the vehicle stopped satisfying the criteria.
    ended_ago_s: NonNegativeFloat | None

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
    # the IDs of the track to which this platform belongs
    track_zone_id: str
    track_zone_numerical_id: int

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
    target_vehicle_type: Literal[VehicleType.TRAM] = VehicleType.TRAM

class Track(BaseTramZone[TrackMetadata]):
    pass

class Platform(BaseTramZone[PlatformMetadata]):
    pass

class IntrusionZone(BaseCarZone[IntrusionZoneMetadata]):
    pass

# --- vehicle states ---

class BaseZoneInfoForVehicle(BaseModel):
    zone_id: str
    zone_numerical_id: int
    # how long the vehicle has been present inside the zone
    present_in_zone_for_s: NonNegativeFloat
    # the statistics for this vehicle's speed in this zone
    speed_in_zone_stats: InZoneSpeeds

class AgnosticZoneInfoForVehicle(BaseZoneInfoForVehicle):
    pass

class TrackInfoForVehicle(BaseZoneInfoForVehicle):
    pass

class PlatformInfoForVehicle(BaseZoneInfoForVehicle):
    pass

class IntrusionZoneInfoForVehicle(BaseZoneInfoForVehicle):
    pass

class BaseZoneInfosForVehicleContainer(BaseModel):
    pass

class AgnosticZoneInfosForVehicleContainer(BaseZoneInfosForVehicleContainer):
    all_zones: List[AgnosticZoneInfoForVehicle]

class ZoneInfosForTramContainer(BaseZoneInfosForVehicleContainer):
    tracks: List[TrackInfoForVehicle]
    platforms: List[PlatformInfoForVehicle]

class ZoneInfosForCarContainer(BaseZoneInfosForVehicleContainer):
    intrusion_zones: List[IntrusionZoneInfoForVehicle]

class BaseVehicleStationaryStats(BaseModel):
    # how long the vehicle was / has been stationary
    duration_s: NonNegativeFloat | None

class CurrentVehicleStationaryStats(BaseVehicleStationaryStats):
    # whether the vehicle is stationary (determined using the global threshold for speed)
    is_stationary: bool | None

class PreviousVehicleStationaryStats(BaseVehicleStationaryStats):
    # time elapsed since the previous time this vehicle was stationary
    ended_ago_s: NonNegativeFloat | None

class VehicleStationaryStats(BaseModel):
    current: CurrentVehicleStationaryStats
    previous: PreviousVehicleStationaryStats

class BaseVehicle[ZoneInfosContainer: BaseZoneInfosForVehicleContainer](BaseModel):

    vehicle_id: str

    # the duration for which this vehicle has been tracked, i. e. its lifetime
    # NOTE: this is NOT the same as how long the vehicle has been present inside the current zone
    # (track, platform, intrusion zone, etc.); the latter is defined in the `zones` field
    present_for_s: NonNegativeFloat

    speed: LifetimeSpeeds

    stationary: VehicleStationaryStats

    zones: ZoneInfosContainer

class AgnosticVehicle(BaseVehicle[AgnosticZoneInfosForVehicleContainer]):
    pass

class Tram(BaseVehicle[ZoneInfosForTramContainer]):
    pass

class Car(BaseVehicle[ZoneInfosForCarContainer]):
    pass

# --- response data model ---

class LiveStateMetadata(BaseModel):
    # the unique ID of the camera
    camera_id: str
    # the unique ID of the current frame
    frame_id: str
    # the timestamp associated with the current frame
    frame_timestamp: datetime

class CommonMetadata(BaseModel):
    # request: APILiveRequestPayload
    server_settings: SceneEventsConfig
    live_state: LiveStateMetadata

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

class BaseVehiclesContainer(BaseModel):
    pass

class AgnosticVehiclesContainer(BaseVehiclesContainer):
    all_vehicles: List[AgnosticVehicle]

class VehiclesContainer(BaseVehiclesContainer):
    trams: List[Tram]
    cars: List[Car]

class BaseLiveAnalyticsState(BaseModel):
    api_version: str = "0.2.0"
    metadata: CommonMetadata

class AgnosticLiveAnalyticsState(BaseLiveAnalyticsState):
    zones: AgnosticZonesContainer
    vehicles: AgnosticVehiclesContainer

class LiveAnalyticsState(BaseLiveAnalyticsState):
    zones: ZonesContainer
    vehicles: VehiclesContainer