from typing import List, Literal
from datetime import datetime

from pydantic import BaseModel, NonNegativeFloat, NonNegativeInt, PositiveInt

from archive.v1.src.api_server.models.scene_state_settings import ServerSettings
from tram_analytics.v1.models.common_types import VehicleType

# --- request payload ---

class APILiveRequestPayload(BaseModel):

    # The ID of the camera for which to request data
    camera_id: str

# --- speeds ---

class Speed(BaseModel):
    current_kmh: NonNegativeFloat | None

class AggregatedSpeedStats(BaseModel):
    # aggregated statistics wrt the vehicle's lifetime
    max_kmh: NonNegativeFloat | None
    mean_kmh: NonNegativeFloat | None
    median_kmh: NonNegativeFloat | None

class SpeedWithStats(Speed):
    aggregated_stats: AggregatedSpeedStats

# --- zones ---

class BaseZone(BaseModel):
    # the unique ID for this zone
    zone_id: str
    # the camera-specific, zone type-specific numerical ID for ease of readability
    # (e. g.: track 2, platform 1, intrusion zone 1)
    zone_numerical_id: str
    # a text description of the zone
    description: str

# --- tram zones (track and platform zones) ---

class BaseTramZone(BaseZone):
    # whether at least one tram is inside the zone
    is_occupied: bool
    # the duration for which the zone has been occupied by trams (None if not presently occupied)
    occupied_for_s: NonNegativeFloat | None
    # the current number of trams inside the zone
    num_vehicles: NonNegativeInt
    # the IDs of the trams inside the zone
    vehicle_ids: List[str]
    # the time elapsed since the last tram's pass (i. e. since when a tram has last left the zone)
    last_pass_ago_s: NonNegativeFloat | None

class Track(BaseTramZone):
    pass

class Platform(BaseTramZone):
    # the ID of the track to which this platform belongs
    track_zone_id: str
    track_zone_numerical_id: int

    # Temporal data for the last completed continuous period when a tram was stationary inside this platform zone.
    # NOTE: The same statistics are tracked for any zone, including tracks and intrusion zones;
    # however, only those for platforms are included in the public API schema for the live state
    # because the others are probably less useful.

    # (1) when this period was completed
    #     - usually corresponds to when the last tram that stopped at this platform departed
    #     - more exactly, when a tram that had been stationary on this platform became moving
    last_stationary_ago_s: float

    # (2) the duration of the period mentioned
    last_stationary_duration_s: float

# --- car zones (intrusion zones) ---

class BaseIntrusion(BaseModel):
    # the duration of the intrusion event
    duration_s: NonNegativeFloat
    # the maximum number of cars registered during the intrusion event
    max_vehicles: PositiveInt

class CurrentIntrusion(BaseIntrusion):
    # the current number of vehicles inside the intrusion zone
    num_vehicles: PositiveInt
    vehicle_ids: List[str]

class LastIntrusion(BaseIntrusion):
    # the time elapsed since the intrusion event was completed
    # (i. e. since the last car that was registered during this event left the intrusion zone)
    ended_ago_s: NonNegativeFloat

class IntrusionsWrapper(BaseModel):
    current: CurrentIntrusion | None
    last: LastIntrusion | None

class IntrusionZone(BaseZone):
    intrusions: IntrusionsWrapper

# --- vehicle states ---

class BaseVehicleZoneInfo(BaseModel):
    zone_id: str
    zone_numerical_id: int
    # how long the vehicle has been present inside the zone
    present_in_zone_for_s: NonNegativeFloat
    # the statistics for this vehicle's speed in this zone
    speed_in_zone_stats: AggregatedSpeedStats

class TramTrackInfo(BaseVehicleZoneInfo):
    pass

class TramPlatformInfo(BaseVehicleZoneInfo):
    pass

class CarIntrusionZoneInfo(BaseVehicleZoneInfo):
    pass

class TramZonesWrapper(BaseModel):
    tracks: List[TramTrackInfo]
    platforms: List[TramPlatformInfo]

class CarZonesWrapper(BaseModel):
    intrusion_zones: List[CarIntrusionZoneInfo]

class BaseVehicle(BaseModel):

    vehicle_id: str

    # the duration for which this vehicle has been tracked, i. e. its lifetime
    # NOTE: this is NOT the same as how long the vehicle has been present inside the current zone
    # (track, platform, intrusion zone, etc.); the latter is defined by subclasses
    present_for_s: NonNegativeFloat

    speed: SpeedWithStats
    # whether the vehicle is stationary (determined using the global threshold for speed)
    is_stationary: bool | None
    # how long the vehicle has been stationary (None if not currently stationary)
    stationary_for_s: NonNegativeFloat | None

class Tram(BaseVehicle):
    vehicle_class: Literal[VehicleType.TRAM] = VehicleType.TRAM
    zones: TramZonesWrapper

class Car(BaseVehicle):
    vehicle_class: Literal[VehicleType.CAR] = VehicleType.CAR
    zones: CarZonesWrapper

# --- response data model ---

class LiveStateMetadata(BaseModel):

    frame_id: str
    # the timestamp associated with the current frame
    frame_timestamp: datetime

class CommonMetadata(BaseModel):

    request: APILiveRequestPayload
    server_settings: ServerSettings
    live_state: LiveStateMetadata

class LiveAnalyticsState(BaseModel):

    api_version: str = "0.1.0"

    metadata: CommonMetadata

    tracks: List[Track]
    platforms: List[Platform]
    intrusion_zones: List[IntrusionZone]

    trams: List[Tram]
    cars: List[Car]