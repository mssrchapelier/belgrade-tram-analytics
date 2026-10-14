from datetime import datetime
from typing import List

from pydantic import BaseModel, Field

from tram_analytics.v1.models.common_types import MotionStatus
from tram_analytics.v1.models.components.scene_state.live_state.speeds import InZoneSpeeds, LifetimeSpeeds


# --- vehicle states ---

class BaseZoneInfoForVehicle(BaseModel):
    """
    Data related to the vehicle's presence in a given zone.
    """
    zone_id: str = Field(description="The unique ID of the zone.")
    zone_numerical_id: int = Field(
        description=(
            "The numerical ID assigned to this zone for ease of readability "
            "that is specific to this scene and zone type "
            "(examples: 2 for Track 2, 3 for Platform 3, 1 for Car intrusion zone 1)."
        )
    )
    description: str = Field(
        description="A textual description for this zone."
    )
    # how long the vehicle has been present inside the zone
    present_in_zone_since_ts: datetime = Field(
        description="The timestamp for the beginning of the vehicle's presence in the zone."
    )
    # the statistics for this vehicle's speed in this zone
    speed_in_zone_stats: InZoneSpeeds

class AgnosticZoneInfoForVehicle(BaseZoneInfoForVehicle):
    pass

class TrackInfoForVehicle(BaseZoneInfoForVehicle):
    pass

class PlatformInfoForVehicle(BaseZoneInfoForVehicle):
    track_zone_id: str = Field(
        description="The unique ID of the tram track to which this platform belongs."
    )

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
    """
    Data related to a stationary period for the vehicle.
    """
    # how long the vehicle was / has been stationary
    start_ts: datetime | None = Field(
        description="The timestamp corresponding to the beginning of the period."
    )

class CurrentVehicleStationaryStats(BaseVehicleStationaryStats):
    """
    Data related to the current stationary period for the vehicle.
    """
    # whether the vehicle is stationary (determined using the global threshold for speed)
    # is_stationary: bool | None
    pass

class PreviousVehicleStationaryStats(BaseVehicleStationaryStats):
    """
    Data related to the previous (i. e. last completed) stationary period for the vehicle.
    """
    # time elapsed since the previous time this vehicle was stationary
    end_ts: datetime | None = Field(
        description="The timestamp corresponding to the end of the period."
    )

class VehicleStationaryStats(BaseModel):
    """
    Data related to periods when the vehicle was continuously stationary.
    """
    current: CurrentVehicleStationaryStats
    previous: PreviousVehicleStationaryStats

class MotionStatusContainer(BaseModel):
    momentary: MotionStatus = Field(
        description=(
            "Momentary motion status, i. e. one determined irrespective of the vehicle's movement history. "
            "For example, a vehicle that has appeared only recently can have a momentary status of 'moving', "
            "but an undefined confirmed motion status."
        )
    )
    confirmed: MotionStatus = Field(
        description=(
            "Confirmed motion status, i. e. one determined based on whether the vehicle's presence "
            "has been confirmed. "
            "For example, a vehicle that has appeared only recently can have a momentary status of 'moving', "
            "but an undefined confirmed motion status."
        )
    )

class MotionInfoContainer(BaseModel):
    """
    Data related to the motion status assigned to the vehicle.
    """
    status: MotionStatusContainer = Field(
        description="The current motion status values (momentary; confirmed)."
    )
    stationary_periods: VehicleStationaryStats

class BaseVehicle[ZoneInfosContainer: BaseZoneInfosForVehicleContainer](BaseModel):

    vehicle_id: str = Field(
        description="The unique ID for this vehicle (preserved for the duration of tracking across frames)."
    )

    # the duration for which this vehicle has been tracked, i. e. its lifetime
    # NOTE: this is NOT the same as how long the vehicle has been present inside the current zone
    # (track, platform, intrusion zone, etc.); the latter is defined in the `zones` field
    present_since_ts: datetime = Field(
        description="The timestamp since which the vehicle has been present on the scene."
    )

    speed: LifetimeSpeeds

    # stationary: VehicleStationaryStats
    motion: MotionInfoContainer

    zones: ZoneInfosContainer = Field(
        description="Data related to the vehicle's presence in zones."
    )

class AgnosticVehicle(BaseVehicle[AgnosticZoneInfosForVehicleContainer]):
    pass

class Tram(BaseVehicle[ZoneInfosForTramContainer]):
    """
    Analytics for a given tram.
    """
    pass

class Car(BaseVehicle[ZoneInfosForCarContainer]):
    """
    Analytics for a given car.
    """
    pass

class BaseVehiclesContainer(BaseModel):
    pass

class AgnosticVehiclesContainer(BaseVehiclesContainer):
    all_vehicles: List[AgnosticVehicle]

class VehiclesContainer(BaseVehiclesContainer):
    """
    Analytical data for vehicles.
    """
    trams: List[Tram]
    cars: List[Car]
