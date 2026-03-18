from datetime import datetime
from typing import List

from pydantic import BaseModel

from tram_analytics.v1.models.common_types import MotionStatus
from tram_analytics.v1.models.components.scene_state.live_state.speeds import InZoneSpeeds, LifetimeSpeeds


# --- vehicle states ---

class BaseZoneInfoForVehicle(BaseModel):
    zone_id: str
    zone_numerical_id: int
    description: str
    # how long the vehicle has been present inside the zone
    present_in_zone_since_ts: datetime
    # the statistics for this vehicle's speed in this zone
    speed_in_zone_stats: InZoneSpeeds

class AgnosticZoneInfoForVehicle(BaseZoneInfoForVehicle):
    pass

class TrackInfoForVehicle(BaseZoneInfoForVehicle):
    pass

class PlatformInfoForVehicle(BaseZoneInfoForVehicle):
    track_zone_id: str

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
    start_ts: datetime | None

class CurrentVehicleStationaryStats(BaseVehicleStationaryStats):
    # whether the vehicle is stationary (determined using the global threshold for speed)
    # is_stationary: bool | None

    pass

class PreviousVehicleStationaryStats(BaseVehicleStationaryStats):
    # time elapsed since the previous time this vehicle was stationary
    end_ts: datetime | None

class VehicleStationaryStats(BaseModel):
    current: CurrentVehicleStationaryStats
    previous: PreviousVehicleStationaryStats

class MotionStatusContainer(BaseModel):
    momentary: MotionStatus
    confirmed: MotionStatus

class MotionInfoContainer(BaseModel):
    status: MotionStatusContainer
    stationary_periods: VehicleStationaryStats

class BaseVehicle[ZoneInfosContainer: BaseZoneInfosForVehicleContainer](BaseModel):

    vehicle_id: str

    # the duration for which this vehicle has been tracked, i. e. its lifetime
    # NOTE: this is NOT the same as how long the vehicle has been present inside the current zone
    # (track, platform, intrusion zone, etc.); the latter is defined in the `zones` field
    present_since_ts: datetime

    speed: LifetimeSpeeds

    # stationary: VehicleStationaryStats
    motion: MotionInfoContainer

    zones: ZoneInfosContainer

class AgnosticVehicle(BaseVehicle[AgnosticZoneInfosForVehicleContainer]):
    pass

class Tram(BaseVehicle[ZoneInfosForTramContainer]):
    pass

class Car(BaseVehicle[ZoneInfosForCarContainer]):
    pass

class BaseVehiclesContainer(BaseModel):
    pass

class AgnosticVehiclesContainer(BaseVehiclesContainer):
    all_vehicles: List[AgnosticVehicle]

class VehiclesContainer(BaseVehiclesContainer):
    trams: List[Tram]
    cars: List[Car]
