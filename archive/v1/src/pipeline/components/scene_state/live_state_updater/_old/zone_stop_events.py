from typing import Set, NamedTuple, Self

from pydantic import BaseModel, model_validator


# --- (2) zone stop events ---

# Rationale:
#     To store:
# (1) the last recorded stop event for every vehicle alive (with the zone IDs);
# (2) the last recorded stop event for every zone (with the vehicle ID),
#     or several such events if they all ended at the same time.
# Information about the duration of such stops is to be exported for all platforms as `last_stop_ago_s`.
#
# Definition:
# - "stop": a vehicle being stationary in the zone for a period exceeding the threshold
#   defined and computed for all zones, but currently exported only for tram platforms
#   (stop events for other zones likely not being of particular interest to the end user)

# vehicle id -> current stationary event [start], completed stationary event [start, end], completed stop event [start, end]
# store only the last completed stop event: (1) for the vehicle, (2) for the zone

# on stationary start:
# - create mapping: vehicle_id -> (vehicle_stationary_start_ts, {zone_ids})
# on zone entrance:
# - create mapping (vehicle_id, zone_id) -> (vehicle_stationary_in_zone_start_ts)
# - update zone_ids in vehicle-to-stop mapping
# on zone exit: (lifetime end or zone exit)
# - pop, add (..., vehicle_stationary_in_zone_end_ts), process
# - if longer than a threshold: update last stop for zone: (start_ts, end_ts, vehicle_id)
# - otherwise, discard
# - do not update zone_ids in vehicle-to-stop mapping (this zone's id stays recorded)
# on stationary end:
# - pop mapping for vehicle, update last stop
# - pop mapping for zone, update last stops
# last for vehicle: (start_ts, end_ts, {zone_ids})
# last for zone: { (start_ts, end_ts, vehicle_id) }

class BaseCompletedEvent(BaseModel):
    start_ts: float
    end_ts: float

    @model_validator(mode="after")
    def _validate_model(self) -> Self:
        if not self.start_ts < self.end_ts:
            raise ValueError("The event start time must be strictly less than end time")
        return self

class CompletedStopEventVehicleRecord(BaseCompletedEvent):
    """
    Represents a completed stop event as recorded for a vehicle.
    """
    # The zones to which this vehicle was assigned during the stop event.
    # Can contain more than one zone if it was moving slowly enough to be considered stationary,
    # but did switch zones during the event.
    zone_ids: Set[str]


class CompletedStopEventZoneRecord(BaseCompletedEvent):
    """
    Represents a completed stop event as recorded for a zone
    (e. g. "what was the last stop event in this zone").
    """
    # The vehicle ID with which this stop event is associated.
    vehicle_id: str


class OngoingStopEventForVehicle:

    def __init__(self, *, start_ts: float, zone_ids: Set[str]) -> None:
        self.start_ts: float = start_ts
        self._zones: Set[str] = zone_ids.copy()

    @property
    def recorded_zones(self) -> Set[str]:
        return self._zones.copy()

    def add_zone(self, zone_id: str) -> None:
        self._zones.add(zone_id)

    # remove_zone not defined: zones are not meant to be removed for this object;
    # it stores all zones in which this vehicle's presence has been recorded
    # during this stop event


class OngoingStopEventForZone(NamedTuple):
    start_ts: float
