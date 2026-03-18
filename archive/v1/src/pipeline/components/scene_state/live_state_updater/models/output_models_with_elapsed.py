from warnings import deprecated

from pydantic import BaseModel

from archive.v1.src.api_server.models.live_analytics_with_elapsed_times_models import (
    CurrentExistenceInZonePeriod, CompletedExistenceInZonePeriod, LastCompletedByVehiclePeriod
)



# --- zones ---

# --- (1) "AT LEAST ONE VEHICLE" periods ---

# Continuous periods during which at least one vehicle, present in the zone, satisfies certain criteria.
# The following are defined below:
# (1) at least one vehicle is present inside the zone;
# (2) at least one vehicle is stationary inside the zone.

# --- (1.1) zone occupancy ---

# Occupancy: a continuous period during which, at any time,
# at least one vehicle of the target class was / has been assigned to this zone.

@deprecated("")
class ZoneCurrentOccupancy(CurrentExistenceInZonePeriod):
    # The current occupancy period, if any.
    #
    # `duration_s`: The duration for which the zone has been occupied by at least one vehicle.
    # null if undefined due to the zone not being presently occupied (for the current period)
    # or no completed occupancy period having been registered yet (for the last period).
    #
    # `max_vehicles`: The maximum number of vehicles that has been registered
    # as SIMULTANEOUSLY being in the zone.
    #
    # `num_vehicles`: The current number of vehicles inside the zone.
    #
    # `vehicle_ids`: The IDs of the vehicles currently in the zone.

    pass


@deprecated("")
class ZoneLastOccupancy(CompletedExistenceInZonePeriod):
    # The previous completed occupancy period, if any.
    pass

# --- (1.2) zone periods with at least one stationary vehicle ---

# Data related to a continuous period during which
# AT LEAST ONE vehicle of the target class, SIMULTANEOUSLY,
# was (has been, for the current such period) stationary inside the zone.

@deprecated("")
class ZoneCurrentPeriodWithStationary(CurrentExistenceInZonePeriod):

    # `duration_s`: for how long this zone has had at least one stationary vehicle
    # registered in it, without interruptions.
    #
    # `max_vehicles`: the maximum number of vehicles that has been
    # registered as SIMULTANEOUSLY being in the zone.

    pass


@deprecated("")
class ZoneLastPeriodWithStationary(CompletedExistenceInZonePeriod):
    pass

# --- (2) "ONE VEHICLE LAST DID THIS ONE TIME" periods ---

# The last completed continuous period during which ANY ONE SINGLE vehicle satisfied certain criteria.
# The following are defined below:
# (1) the last period during which any one single vehicle was stationary inside the zone
#     (i. e. a tram stopping at the platform:
#     the duration of and time elapsed from the end of last such stop)
# (2) "pass": the last period during which any one single vehicle was present inside the zone
#     (i. e. a tram passing through a specified zone:
#     the duration of last such pass and the time elapsed since it ended)

# --- (2.1) the last vehicle stationary inside the zone ---

@deprecated("")
class ZoneLastStop(LastCompletedByVehiclePeriod):

    # Data related to: the last completed continuous period during which
    # ANY ONE SINGLE vehicle was stationary inside the zone.
    #
    # `ended_ago_s`: The time elapsed since the last moment when a vehicle
    # that had been stationary in the zone ceased to be stationary
    # (whilst in the zone); e. g., for tram platforms,
    # since the last tram's stop at the platform ended.
    #
    # `duration_s`: The duration of the period for which
    # the vehicle mentioned above was stationary inside the zone.

    pass

# --- (2.2) the last vehicle passing the zone ---

@deprecated("")
class ZoneLastPass(LastCompletedByVehiclePeriod):

    # Data related to the last completed pass by any vehicle of the target class.
    # "Pass": a continuous period between any specific vehicle's entry into and exit from the zone.
    #
    # `ended_ago_s`: The time elapsed since the last pass
    # (i. e. since when a vehicle has last left the zone).
    #
    # `duration_s`: The duration of the last pass.

    pass

# --- containers for zone periods ---

@deprecated("")
class ZoneOccupancyPeriods(BaseModel):
    current_period: ZoneCurrentOccupancy
    last_completed_period: ZoneLastOccupancy

@deprecated("")
class ZoneOccupancy(BaseModel):
    continuously_occupied: ZoneOccupancyPeriods
    last_completed_vehicle_pass: ZoneLastPass

@deprecated("")
class ZoneStationaryVehiclesPresencePeriods(BaseModel):
    current_period: ZoneCurrentPeriodWithStationary
    last_completed_period: ZoneLastPeriodWithStationary

@deprecated("")
class ZoneStationaryVehiclesPresence(BaseModel):
    continuously_with_stopped_vehicles: ZoneStationaryVehiclesPresencePeriods
    last_completed_vehicle_stop: ZoneLastStop