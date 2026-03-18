from typing import List
from pydantic import BaseModel, NonNegativeFloat, NonNegativeInt

class TramZone(BaseModel):
    """
    An abstraction for tram tracks and tram platforms.
    """

    # a text description for the zone
    description: str
    # whether the zone contains at least one vehicle belonging to the class of interest (tram/car)
    is_occupied: bool
    # if occupied: for how long
    occupied_for_s: NonNegativeFloat | None
    # the number of vehicles currently inside the zone
    num_vehicles_current: NonNegativeInt
    # the IDs of the vehicles inside the zone
    occupied_by_vehicle_ids: List[str]
    # the time elapsed since the last vehicle left the zone
    last_clear_ago_s: NonNegativeFloat

class TramTrack(TramZone):
    track_id: int

class TramPlatform(TramZone):
    platform_id: int
    track_id: int
    # the duration of the STATIONARY TIME for the last (or the current one if one is in progress) STOPPING pass
    # TODO: how to define if there is more than one tram on the platform?
    last_dwell_s: NonNegativeFloat

class Intrusion(BaseModel):
    duration_s: NonNegativeFloat
    max_cars: NonNegativeInt

class CurrentIntrusion(Intrusion):
    num_cars: NonNegativeInt
    car_ids: List[str]

class LastIntrusion(Intrusion):
    ended_ago_s: NonNegativeFloat

class IntrusionZone(BaseModel):
    intrusion_zone_id: int
    description: str
    is_active_intrusion: bool
    current_intrusion: CurrentIntrusion | None
    last_intrusion: LastIntrusion | None

class BaseVehicle(BaseModel):
    vehicle_id: str
    # the duration of the presence of this vehicle:
    # - for trams: in the current track zone
    # - for cars: in the current intrusion zone
    # Note: The semantics of `present_for_s` should probably be refined later
    present_for_s: NonNegativeFloat
    cur_speed_kmh: NonNegativeFloat | None
    # whether the current speed is below the threshold
    is_stationary: bool
    # the duration for which the vehicle has been stationary
    stationary_for_s: NonNegativeFloat | None

class Tram(BaseVehicle):
    track_id: int
    is_at_platform: bool
    platform_id: int | None
    at_platform_for_s: NonNegativeFloat | None

class Car(BaseVehicle):
    # The median of all speeds (smoothed as per the smoothing policy employed)
    # recorded in the current lifetime of this vehicle.
    # Note: Can be computed for BaseVehicle (i. e. for trams as well),
    # but probably of less interest, so only included here.
    median_moving_speed_kmh: NonNegativeFloat | None

class LiveState(BaseModel):
    api_version: str = "0.1.0"

    camera_id: str
    frame_id: str
    timestamp_utc_iso: str

    tracks: List[TramTrack]
    platforms: List[TramPlatform]
    intrusion_zones: List[IntrusionZone]
    trams: List[Tram]
    cars: List[Car]