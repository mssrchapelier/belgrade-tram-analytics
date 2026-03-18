from typing import (
    List, Dict, Set, Self, DefaultDict, override, NamedTuple
)
from abc import ABC, abstractmethod
from dataclasses import dataclass
from collections import defaultdict

from pydantic import PositiveInt

from common.utils.time_utils import datetime_to_utc_posix
from common.utils.dynamic_median_updater import DynamicMedianUpdater
from tram_analytics.v1.pipeline.components.scene_state.live_state_updater.config.zones_config import SingleZoneConfig, ZonesConfig
from archive.v1.src.pipeline.components.scene_state.live_state_updater._old_2.live_state_updater import BaseCompletedEvent_Old
from tram_analytics.v1.models.common_types import ZoneType, VehicleType
from archive.v1.src.pipeline.components.scene_state.events._old.events_old import (
    EventBoundaryType, LifetimeEvent, ZoneOccupancyEvent, SpeedUpdateEvent, EventsContainer
)
from tram_analytics.v1.models.components.scene_state.live_state.live_state import LiveAnalyticsState
from archive.v1.src.api_server.models.scene_state_settings import SpeedType, ServerSettings

# --- vehicle states ---

# --- (1) speed states ---

class BaseSpeedMetricUpdater(ABC):

    @property
    @abstractmethod
    def value(self) -> float | None:
        """
        A getter for the current value of the computed aggregated parameter.
        """
        pass

    @abstractmethod
    def _update_with_unmatched(self, current: float | None) -> None:
        """
        Meant to be called whenever the current state is unmatched.
        Should update the aggregated parameter accordingly.
        """
        pass

    @abstractmethod
    def _update_with_matched(self, current: float | None) -> None:
        """
        Meant to be called whenever the current state is matched.
        Should update the aggregated parameter accordingly.
        """
        pass

    def update(self, speed: float | None, is_matched: bool) -> float | None:
        """
        Updates this object with the current state's parameters
        and returns the recalculated aggregated parameter's value.

        :param speed: the current speed value (`None` if undefined)
        :param is_matched: whether, for this frame, the vehicle is associated with an actual detection
        :returns: the recalculated value of the computed aggregated parameter
        """
        if is_matched:
            self._update_with_matched(speed)
        else:
            self._update_with_unmatched(speed)
        return self.value

class CurrentSpeedUpdater(BaseSpeedMetricUpdater):

    def __init__(self):
        self._current_speed: float | None = None

    @override
    @property
    def value(self) -> float | None:
        return self._current_speed

    @override
    def _update_with_unmatched(self, current: float | None) -> None:
        self._current_speed = current

    @override
    def _update_with_matched(self, current: float | None) -> None:
        self._current_speed = current

class MaxSpeedUpdater(BaseSpeedMetricUpdater):

    def __init__(self):
        self._max_over_confirmed_lifetime: float | None = None
        self._max_over_last_unmatched: float | None = None

    @override
    @property
    def value(self) -> float | None:
        return self._max_over_confirmed_lifetime

    def _reset_last_unmatched(self) -> None:
        self._max_over_last_unmatched = None

    @override
    def _update_with_unmatched(self, current: float | None) -> None:
        if current is None:
            return
        if self._max_over_last_unmatched is None:
            self._max_over_last_unmatched = current
        else:
            self._max_over_last_unmatched = max(self._max_over_last_unmatched,
                                                current)

    @override
    def _update_with_matched(self, current: float | None) -> None:
        """
        Meant to be called when the current state is MATCHED and its speed is defined.
        """
        max_over: List[float] = []
        if current is not None:
            max_over.append(current)
        if self._max_over_confirmed_lifetime is not None:
            max_over.append(self._max_over_confirmed_lifetime)
        if self._max_over_last_unmatched is not None:
            max_over.append(self._max_over_last_unmatched)

        self._max_speed = max(max_over)

        self._reset_last_unmatched()

class MeanSpeedUpdater(BaseSpeedMetricUpdater):

    # Dynamically updates the mean speed over the vehicle's confirmed lifetime.
    # More efficient than recalculating the mean over the entire history,
    # which, for any given step, is O(n) bound on the length of history at that step;
    # this algorithm is O(1).

    def __init__(self):
        # Note: _len_of_... do not include states where the speed value was null.
        self._mean_over_confirmed_lifetime: float | None = None
        self._len_of_confirmed_lifetime: int = 0
        self._mean_over_last_unmatched: float | None = None
        self._len_of_last_unmatched: int = 0

    @override
    @property
    def value(self) -> float | None:
        return self._mean_over_confirmed_lifetime

    def _reset_last_unmatched(self) -> None:
        self._mean_over_last_unmatched = None
        self._len_of_last_unmatched = 0

    @override
    def _update_with_unmatched(self, current: float | None) -> None:
        """
        Meant to be called when the current state is UNMATCHED and its speed is defined.
        """
        if current is None:
            return

        if self._mean_over_last_unmatched is None:
            if not self._len_of_last_unmatched == 0:
                nonzero_len_msg: str = ("Inconsistent state: mean over last unmatched is null "
                                        "but length of last unmatched is not 0")
                raise RuntimeError(nonzero_len_msg)

            self._mean_over_last_unmatched = current
        else:
            if self._len_of_last_unmatched == 0:
                zero_len_msg: str = ("Inconsistent state: mean over last unmatched is not null "
                                     "but length of last unmatched is 0")
                raise RuntimeError(zero_len_msg)

            new_mean_numerator: float = (self._mean_over_last_unmatched * self._len_of_last_unmatched) + current
            new_mean_denominator: float = self._len_of_last_unmatched + 1
            new_mean: float = new_mean_numerator / new_mean_denominator
            self._mean_over_last_unmatched = new_mean

        self._len_of_last_unmatched += 1

    @override
    def _update_with_matched(self, current: float | None) -> None:
        """
        Meant to be called when the current state is MATCHED and its speed is defined.
        """

        # --- numerator ---
        num_lifetime_term: float = (
            self._mean_over_confirmed_lifetime * self._len_of_confirmed_lifetime
            if self._mean_over_confirmed_lifetime is not None
            else 0.0
        )
        num_last_unmatched_term: float = (
            self._mean_over_last_unmatched * self._len_of_last_unmatched
            if self._mean_over_last_unmatched is not None
            else 0.0
        )
        num_current_term: float = current if current is not None else 0.0
        numerator: float = num_lifetime_term + num_last_unmatched_term + num_current_term

        # --- denominator ---
        denom_lifetime_term: float = float(self._len_of_confirmed_lifetime)
        denom_last_unmatched_term: float = float(self._len_of_last_unmatched)
        denom_current_term: float = 1.0 if current is not None else 0.0
        denominator: float = denom_lifetime_term + denom_last_unmatched_term + denom_current_term

        # --- value ---
        new_mean: float | None = (numerator / denominator if denominator != 0.0
                                  else None)

        # updating lifetime params
        self._mean_over_confirmed_lifetime = new_mean
        if current is not None:
            self._len_of_confirmed_lifetime += 1

        # resetting last unmatched sequence params
        self._reset_last_unmatched()

class MedianSpeedUpdater(BaseSpeedMetricUpdater):

    # Dynamically updates the median speed over the vehicle's confirmed lifetime.
    # More efficient than recalculating the median over the entire history;
    # this algorithm utilises the two-heap approach for dynamic updates of the median.

    def __init__(self):
        # Note: _len_of_... do not include states where the speed value was null.
        self._updater_over_confirmed_lifetime: DynamicMedianUpdater = DynamicMedianUpdater()
        self._updater_over_last_unmatched: DynamicMedianUpdater = DynamicMedianUpdater()

    @override
    @property
    def value(self) -> float | None:
        return self._updater_over_confirmed_lifetime.value

    def _reset_last_unmatched(self) -> None:
        self._updater_over_last_unmatched.reset()

    @override
    def _update_with_unmatched(self, current: float | None) -> None:
        if current is None:
            return
        self._updater_over_last_unmatched.update(current)

    @override
    def _update_with_matched(self, current: float | None) -> None:
        # track reanimated: transfer all values in the preceding unmatched sequence to confirmed lifetime
        for value in self._updater_over_last_unmatched: # type: float
            self._updater_over_confirmed_lifetime.update(value)
        if current is not None:
            self._updater_over_confirmed_lifetime.update(current)

class SpeedMetrics(NamedTuple):
    current: float | None
    max: float | None
    mean: float | None
    median: float | None

class SpeedMetricUpdaterContainer:

    def __init__(self) -> None:
        self._updater_current: CurrentSpeedUpdater = CurrentSpeedUpdater()
        self._updater_max: MaxSpeedUpdater = MaxSpeedUpdater()
        self._updater_mean: MeanSpeedUpdater = MeanSpeedUpdater()
        self._updater_median: MedianSpeedUpdater = MedianSpeedUpdater()

        self._updaters: List[BaseSpeedMetricUpdater] = [
            self._updater_current, self._updater_max, self._updater_mean, self._updater_median
        ]

        self._current_values: SpeedMetrics | None = None

    @property
    def values(self) -> SpeedMetrics | None:
        return self._current_values

    def update(self, speed: float | None, is_matched: bool) -> SpeedMetrics:
        for updater in self._updaters: # type: BaseSpeedMetricUpdater
            updater.update(speed, is_matched)
        metrics: SpeedMetrics = SpeedMetrics(current=self._updater_current.value,
                                             max=self._updater_max.value,
                                             mean=self._updater_mean.value,
                                             median=self._updater_median.value)
        self._current_values = metrics
        return metrics

class RawSmoothedSpeedMetrics(NamedTuple):
    raw: SpeedMetrics
    smoothed: SpeedMetrics

class RawSmoothedWrapperSpeedMetricUpdaterContainer:

    def __init__(self) -> None:
        self._updater_raw: SpeedMetricUpdaterContainer = SpeedMetricUpdaterContainer()
        self._updater_smoothed: SpeedMetricUpdaterContainer = SpeedMetricUpdaterContainer()

        self._current_values: RawSmoothedSpeedMetrics | None = None

    @property
    def values(self) -> RawSmoothedSpeedMetrics | None:
        return self._current_values

    def update(self,
               *, speed_raw: float | None,
               speed_smoothed: float | None,
               is_matched: bool) -> RawSmoothedSpeedMetrics:
        metrics_for_raw: SpeedMetrics = self._updater_raw.update(speed_raw, is_matched)
        metrics_for_smoothed: SpeedMetrics = self._updater_smoothed.update(speed_smoothed, is_matched)
        metrics: RawSmoothedSpeedMetrics = RawSmoothedSpeedMetrics(
            raw=metrics_for_raw, smoothed=metrics_for_smoothed
        )
        self._current_values = metrics
        return metrics

# --- (2) zone-related info for a single vehicle ---

class VehicleSingleZoneInfo:

    """
    For a single zone to which the vehicle is assigned,
    updates speed-based aggregated parameters
    for the duration of the vehicle's current dwell in that zone
    (current -- not meant to be used; max, mean, median).
    """

    def __init__(self, zone_entrance_ts: float) -> None:

        # zone_id: str

        self.vehicle_zone_enter_ts: float = zone_entrance_ts
        # self.max_speeds_in_zone: MaxSpeeds = MaxSpeeds()
        self._speed_updater: RawSmoothedWrapperSpeedMetricUpdaterContainer = RawSmoothedWrapperSpeedMetricUpdaterContainer()

    @property
    def speeds(self) -> RawSmoothedSpeedMetrics | None:
        return self._speed_updater.values

    def update(self, *,
               speed_raw: float | None,
               speed_smoothed: float | None,
               is_matched: bool) -> None:
        # self.max_speeds_in_zone.update(raw=speed_raw, smoothed=speed_smoothed)
        self._speed_updater.update(speed_raw=speed_raw,
                                   speed_smoothed=speed_smoothed,
                                   is_matched=is_matched)

class VehicleZonesInfo:

    """
    Keeps track of the zones to which a single vehicle is assigned.
    For every zone, updates speed-based aggregated parameters
    for the duration of the vehicle's current dwell in that zone
    (current -- not meant to be used; max, mean, median).
    """

    def __init__(self) -> None:
        self.zones: Dict[str, VehicleSingleZoneInfo] = dict()

    def add_zone(self, *, zone_id: str, zone_entrance_ts: float) -> None:
        if zone_id in self.zones:
            raise ValueError(f"Cannot add zone {zone_id}: already exists in the state")
        self.zones[zone_id] = VehicleSingleZoneInfo(zone_entrance_ts)

    def update_speeds_in_registered_zones(self, *, speed_raw: float | None,
                                          speed_smoothed: float | None,
                                          is_matched: bool) -> None:
        for zone in self.zones.values(): # type: VehicleSingleZoneInfo
            zone.update(speed_raw=speed_raw,
                        speed_smoothed=speed_smoothed,
                        is_matched=is_matched)

    def remove_zone(self, zone_id: str) -> None:
        if zone_id not in self.zones:
            raise ValueError(f"Cannot remove zone {zone_id}: does not exist in the state")
        self.zones.pop(zone_id)

# --- (3) vehicle state object ---

class StationaryDeterminationSettings(NamedTuple):
    speed_type_for_stationary_determination: SpeedType
    is_stationary_threshold_kmh: float

class VehicleState:

    """
    For the given vehicle:
    (1) stores the start timestamp for its lifetime;
    (2) updates speed-based aggregated parameters for the duration of its lifetime
        (current speed; max, mean, median over the vehicle's lifetime -- dynamically updated);
    (3) keeps track of the zones to which the vehicle is assigned, adds/removes them as necessary,
        and updates speed-based aggregated parameters for the duration of the vehicle's dwell in the zone
        (max, mean, median over the duration of the dwell -- dynamically updated).
    """

    def __init__(self, *, lifetime_start_ts: float, settings: StationaryDeterminationSettings) -> None:
        self._settings: StationaryDeterminationSettings = settings

        self.lifetime_start_ts: float = lifetime_start_ts
        self.lifetime_speed_stats: RawSmoothedWrapperSpeedMetricUpdaterContainer = RawSmoothedWrapperSpeedMetricUpdaterContainer()

        self.stationary_start_ts: float | None = None
        self._is_stationary: bool | None = None

        self.zone_speed_stats: VehicleZonesInfo = VehicleZonesInfo()

    def add_zone(self, *, zone_id: str, zone_entrance_ts: float) -> None:
        self.zone_speed_stats.add_zone(zone_id=zone_id,
                                       zone_entrance_ts=zone_entrance_ts)

    def remove_zone(self, zone_id: str) -> None:
        self.zone_speed_stats.remove_zone(zone_id)

    def _update_is_stationary(self, current_ts: float, cur_is_stationary: bool) -> None:
        if self._is_stationary is None and self.stationary_start_ts is not None:
            raise RuntimeError("Inconsistent state: this object's _is_stationary is undefined "
                               "but stationary_start_ts is defined")

        prev_is_stationary: bool | None = self._is_stationary
        self._is_stationary = cur_is_stationary
        if not cur_is_stationary:
            self.stationary_start_ts = None
        elif not prev_is_stationary:
            # record the start of a new stationary period
            self.stationary_start_ts = current_ts

    def _select_raw_or_smoothed(self,
                                *, speed_raw: float | None,
                                speed_smoothed: float | None) -> float | None:
        # choose whether to update based on the raw or smoothed speed based on the setting passed
        speed_type: SpeedType = self._settings.speed_type_for_stationary_determination
        match speed_type:
            case SpeedType.RAW:
                return speed_raw
            case SpeedType.SMOOTHED:
                return speed_smoothed
            case _:
                raise ValueError(f"Unsupported speed type for determination "
                                 f"of moving vs stationary status: {speed_type}")

    def _compute_is_stationary(self, *, speed_raw: float | None,
                               speed_smoothed: float | None,
                               is_matched: bool) -> bool | None:
        if not is_matched:
            # do not update the stationary status based on unmatched track states
            return None
        which: float | None = self._select_raw_or_smoothed(speed_raw=speed_raw,
                                                           speed_smoothed=speed_smoothed)
        if which is None:
            # do not update the stationary status based on undefined speed values
            return None
        is_stationary: bool = which <= self._settings.is_stationary_threshold_kmh
        return is_stationary

    def update_speeds_and_stationary_status(self,
                                            *, current_ts: float,
                                            speed_raw: float | None,
                                            speed_smoothed: float | None,
                                            is_matched: bool) -> None:
        # NOTE: Does NOT update zone IDs (must be updated in the calling code first)
        self.lifetime_speed_stats.update(
            speed_raw=speed_raw, speed_smoothed=speed_smoothed, is_matched=is_matched
        )
        self.zone_speed_stats.update_speeds_in_registered_zones(
            speed_raw=speed_raw, speed_smoothed=speed_smoothed, is_matched=is_matched
        )
        is_stationary: bool | None = self._compute_is_stationary(
            speed_raw=speed_raw, speed_smoothed=speed_smoothed, is_matched=is_matched
        )
        if is_stationary is not None:
            self._update_is_stationary(current_ts=current_ts, cur_is_stationary=is_stationary)

# --- zone states (for all zones) ---

# --- (1) zone occupancy events ---

class OccupancyEventEndedException(Exception):
    pass

class OngoingOccupancyEventState:

    """
    An object representing the mutable state of an ongoing zone occupancy event,
    i. e. an event consisting in a particular zone being occupied by at least one vehicle.
    """

    def __init__(self, *, start_ts: float, vehicle_id: str) -> None:
        # NOTE: Meant to be called whenever an event "vehicle entered zone" is created.
        # As such, always initialised with one vehicle.

        self.start_ts: float = start_ts
        self.vehicle_ids: Set[str] = {vehicle_id}
        self.num_vehicles: int = len(self.vehicle_ids)
        self.max_vehicles: int = self.num_vehicles

        # Whether this event has ended.
        # This flag is set to `True` when `num_vehicles` reaches 0.
        # After the flag has been set to `True`, methods to change this event's state
        # should raise an `OccupancyEventEndedException`.
        self.has_ended: bool = False

    def _update_max_vehicles(self) -> None:
        self.max_vehicles = max(self.num_vehicles,
                                self.max_vehicles)

    def add_vehicle(self, vehicle_id: str) -> None:
        if self.has_ended:
            raise OccupancyEventEndedException()
        if vehicle_id in self.vehicle_ids:
            raise ValueError(f"Can't add vehicle {vehicle_id} to vehicle IDs: already present")
        self.vehicle_ids.add(vehicle_id)
        self.num_vehicles = len(self.vehicle_ids)
        self._update_max_vehicles()

    def remove_vehicle(self, vehicle_id: str) -> None:
        if self.has_ended:
            raise OccupancyEventEndedException()
        if vehicle_id not in self.vehicle_ids:
            raise ValueError(f"Can't remove vehicle {vehicle_id} from vehicle IDs: not present")
        self.vehicle_ids.remove(vehicle_id)
        self.num_vehicles = len(self.vehicle_ids)
        self._update_max_vehicles()
        if self.num_vehicles == 0:
            self.has_ended = True


class CompletedOccupancyEvent(BaseCompletedEvent_Old):

    """
    An object representing the state of a completed zone occupancy event.
    """

    max_vehicles: PositiveInt

    @classmethod
    def from_occupancy_event_state(cls,
                                   *, state: OngoingOccupancyEventState,
                                   end_ts: float) -> Self:
        return cls(start_ts=state.start_ts,
                   end_ts=end_ts,
                   max_vehicles=state.max_vehicles)

class ZoneOccupancyState:
    """
    An object representing a zone, maintaining data about:
    (1) the current occupancy event, if any;
    (2) the previous occupancy event, if any.
    """

    def __init__(self) -> None:
        self.current_occupancy: OngoingOccupancyEventState | None = None
        self.previous_occupancy: CompletedOccupancyEvent | None = None

    # vehicle entered zone
    def add_vehicle(self, *, event_ts: float, vehicle_id: str) -> None:
        # if there is no current occupancy event: create one
        if self.current_occupancy is None:
            self.current_occupancy = OngoingOccupancyEventState(
                start_ts=event_ts, vehicle_id=vehicle_id
            )
        # if there is one, update it
        else:
            self.current_occupancy.add_vehicle(vehicle_id)

    # vehicle exited zone
    def remove_vehicle(self, *, event_ts: float, vehicle_id: str) -> None:
        # if there is no current occupancy event: raise an exception
        if self.current_occupancy is None:
            msg: str = f"Can't remove vehicle ID {vehicle_id} from the zone: the zone is currently registered is unoccupied"
            raise RuntimeError(msg)
        # remove the vehicle ID from the occupancy event
        self.current_occupancy.remove_vehicle(vehicle_id)
        # if the occupancy event has ended:
        if self.current_occupancy.has_ended:
            # update the previous occupancy event
            self.previous_occupancy = CompletedOccupancyEvent.from_occupancy_event_state(
                state=self.current_occupancy, end_ts=event_ts
            )
            # set the current occupancy event to None
            self.current_occupancy = None

# --- live state updater ---

# --- (1) internal mappings for zones and vehicles ---

@dataclass(frozen=True, slots=True, kw_only=True)
class BaseZonesMappings:
    """
    Mappings agnostic wrt zone types.
    """

    # ZoneType -> { zone ids }
    type_to_ids: Dict[ZoneType, Set[str]]
    # zone id -> ZoneType
    id_to_type: Dict[str, ZoneType]
    # zone id -> zone numerical id
    id_to_num_id: Dict[str, int]
    # id -> description
    id_to_description: Dict[str, str]

@dataclass(frozen=True, slots=True, kw_only=True)
class ZonesMappings(BaseZonesMappings):
    """
    Mappings whose semantics are specific to the currently specified zone types.
    """

    # platform id -> track id
    ids_platform_to_track: Dict[str, str]
    # track id -> { platform ids }
    ids_track_to_platforms: Dict[str, Set[str]]
    # zone id -> ZoneState
    states: Dict[str, ZoneOccupancyState]

@dataclass(slots=True, kw_only=True)
class VehiclesMappings:
    # VehicleType -> { vehicle ids }
    type_to_ids: Dict[VehicleType, Set[str]]
    # vehicle id -> VehicleType
    id_to_type: Dict[str, VehicleType]
    # vehicle id -> VehicleState
    states: Dict[str, VehicleState]

# --- (2) master object ---

class LiveStateUpdater:

    def __init__(self,
                 *, camera_id: str,
                 zones_config: ZonesConfig,
                 settings: ServerSettings) -> None:
        self._camera_id: str = camera_id
        self._stationary_calc_settings: StationaryDeterminationSettings = StationaryDeterminationSettings(
            speed_type_for_stationary_determination=settings.speed_type_for_stationary_determination,
            is_stationary_threshold_kmh=settings.is_stationary_threshold_ms
        )

        # the current frame ID
        self._cur_frame_id: str | None = None
        self._cur_frame_ts: float | None = None
        self._prev_frame_ts: float | None = None

        # zone states: maintained for all zones defined in `zones_config`
        self.zones: ZonesMappings = self._init_zones(zones_config)
        # vehicle states: maintained for currently existing vehicles
        self.vehicles: VehiclesMappings = self._init_vehicles_mappings()


    # --- state validation methods ---

    def _check_frame_initialised(self) -> None:
        if self._cur_frame_id is None or self._cur_frame_ts is None:
            raise RuntimeError("frame_id and/or current_ts are None: set to current values first "
                               "before updating the instance's state")

    def _check_vehicle_not_initialised(self, vehicle_id: str) -> None:
        if vehicle_id in self.vehicles.states:
            raise RuntimeError(f"Vehicle {vehicle_id} has been initialised: "
                               f"already exists in vehicles.states")
        if vehicle_id in self.vehicles.id_to_type:
            raise RuntimeError(f"Vehicle {vehicle_id} has been initialised: "
                               f"already exists in vehicles.id_to_type")
        for vehicle_type in self.vehicles.type_to_ids: # type: VehicleType
            if vehicle_id in self.vehicles.type_to_ids[vehicle_type]:
                raise RuntimeError(f"Vehicle {vehicle_id} has been initialised: "
                                   f"already exists in vehicles.type_to_ids (type {vehicle_type})")

    def _check_vehicle_initialised(self, vehicle_id: str) -> None:
        if vehicle_id not in self.vehicles.states:
            raise RuntimeError(f"Vehicle {vehicle_id} has not been initialised: "
                               f"does not exist in vehicles.states")
        if vehicle_id not in self.vehicles.id_to_type:
            raise RuntimeError(f"Vehicle {vehicle_id} has not been initialised: "
                               f"does not exist in vehicles.id_to_type")
        if not any(
                vehicle_id in self.vehicles.type_to_ids[vehicle_type]
                for vehicle_type in self.vehicles.type_to_ids
        ):
            raise RuntimeError(f"Vehicle {vehicle_id} has not been initialised: "
                               f"does not exist in vehicles.type_to_ids")

    def _check_zone_registered(self, zone_id: str) -> None:
        if not zone_id in self.zones.id_to_type:
            raise ValueError(f"Zone {zone_id} not registered")

    # --- event processing methods ---

    def _start_vehicle_lifetime(self,
                                *, frame_ts: float,
                                vehicle_type: VehicleType,
                                vehicle_id: str) -> None:
        self._check_frame_initialised()
        self._check_vehicle_not_initialised(vehicle_id)
        # vehicle_state_type: Type[VehicleState_Old] = _vehicle_state_type_from_vehicle_type(vehicle_type)

        # create a new vehicle state object for the given vehicle id
        # self.vehicles.states[vehicle_id] = vehicle_state_type(self._cur_frame_ts)
        self.vehicles.states[vehicle_id] = VehicleState(
            lifetime_start_ts=frame_ts,
            settings=self._stationary_calc_settings
        )
        # register the mapping from the new vehicle id to the given vehicle type
        self.vehicles.id_to_type[vehicle_id] = vehicle_type
        # register the new vehicle id under the given vehicle type
        self.vehicles.type_to_ids[vehicle_type].add(vehicle_id)

    def _end_vehicle_lifetime(self, vehicle_id: str) -> None:
        self._check_frame_initialised()
        self._check_vehicle_initialised(vehicle_id)

        # unregister the vehicle state
        self.vehicles.states.pop(vehicle_id)
        # unregister vehicle type mappings
        vehicle_type: VehicleType = self.vehicles.id_to_type.pop(vehicle_id)
        self.vehicles.type_to_ids[vehicle_type].remove(vehicle_id)

    def _register_zone_entrance(self, *, frame_ts: float, vehicle_id: str, zone_id: str) -> None:
        self._check_frame_initialised()
        self._check_vehicle_initialised(vehicle_id)
        self._check_zone_registered(zone_id)

        vehicle_state: VehicleState = self._get_vehicle_state(vehicle_id)
        zone_state: ZoneOccupancyState = self._get_zone_state(zone_id)

        # update vehicle state with zone
        vehicle_state.add_zone(zone_id=zone_id, zone_entrance_ts=frame_ts)
        # update zone state with vehicle
        zone_state.add_vehicle(event_ts=frame_ts, vehicle_id=vehicle_id)

    def _register_zone_exit(self, *, frame_ts: float, vehicle_id: str, zone_id: str) -> None:
        # vehicle may or may not be initialised as per the result of `_check_vehicle_initialised()`:
        # this method should be called, in particular, after removing the vehicle from the vehicle mappings
        self._check_frame_initialised()
        self._check_zone_registered(zone_id)

        vehicle_state: VehicleState = self._get_vehicle_state(vehicle_id)
        zone_state: ZoneOccupancyState = self._get_zone_state(zone_id)

        # update vehicle state with zone
        vehicle_state.remove_zone(zone_id)
        # update zone state with vehicle
        zone_state.remove_vehicle(event_ts=frame_ts, vehicle_id=vehicle_id)

    def _update_vehicle_speeds(self, *,
                               frame_ts: float,
                               vehicle_id: str,
                               speed_raw: float | None,
                               speed_smoothed: float | None,
                               is_matched: bool):
        self._check_frame_initialised()
        self._check_vehicle_initialised(vehicle_id)

        vehicle_state: VehicleState = self._get_vehicle_state(vehicle_id)

        vehicle_state.update_speeds_and_stationary_status(
            current_ts=frame_ts, speed_raw=speed_raw, speed_smoothed=speed_smoothed,
            is_matched=is_matched
        )

    # --- state initialisation helpers ---

    @staticmethod
    def _init_zones(config: ZonesConfig) -> ZonesMappings:
        # ZoneType -> { zone ids }
        type_to_ids: DefaultDict[ZoneType, Set[str]] = defaultdict(set)
        # zone id -> ZoneType
        id_to_type: Dict[str, ZoneType] = dict()
        # zone id -> zone numerical id
        id_to_num_id: Dict[str, int] = dict()
        # id -> description
        id_to_description: Dict[str, str] = dict()
        # platform id -> track id
        ids_platform_to_track: Dict[str, str] = dict()
        # track id -> { platform ids }
        ids_track_to_platforms: DefaultDict[str, Set[str]] = defaultdict(set)
        # zone id -> ZoneState
        states: Dict[str, ZoneOccupancyState] = dict()

        zone_configs: List[SingleZoneConfig] = [*config.tracks,
                                                *config.platforms,
                                                *config.intrusion_zones]

        for zone in zone_configs: # type: SingleZoneConfig
            zone_id: str = zone.zone_id
            zone_type: ZoneType = zone.zone_type
            type_to_ids[zone_type].add(zone_id)
            id_to_type[zone_id] = zone_type
            id_to_num_id[zone_id] = zone.zone_numerical_id
            id_to_description[zone_id] = zone.description
            states[zone_id] = ZoneOccupancyState()
            if zone.zone_type is ZoneType.PLATFORM:
                track_id: str = zone.track_zone_id
                ids_platform_to_track[zone_id] = track_id
                ids_track_to_platforms[track_id].add(zone_id)

        mappings: ZonesMappings = ZonesMappings(type_to_ids=type_to_ids,
                                                id_to_type=id_to_type,
                                                id_to_num_id=id_to_num_id,
                                                id_to_description=id_to_description,
                                                ids_platform_to_track=ids_platform_to_track,
                                                ids_track_to_platforms=ids_track_to_platforms,
                                                states=states)
        return mappings

    @staticmethod
    def _init_vehicles_mappings() -> VehiclesMappings:
        # VehicleType -> { vehicle ids }
        vehicle_type_to_ids: Dict[VehicleType, Set[str]] = {
            v_type: set()
            for v_type in VehicleType
        }
        # vehicle id -> VehicleType
        vehicle_id_to_type: Dict[str, VehicleType] = dict()
        # vehicle id -> VehicleState
        states: Dict[str, VehicleState] = dict()
        mappings: VehiclesMappings = VehiclesMappings(type_to_ids=vehicle_type_to_ids,
                                                      id_to_type=vehicle_id_to_type,
                                                      states=states)
        return mappings

    # --- misc helpers ---

    def _get_vehicle_state(self, vehicle_id: str) -> VehicleState:
        return self.vehicles.states[vehicle_id]

    def _get_zone_state(self, zone_id: str) -> ZoneOccupancyState:
        return self.zones.states[zone_id]

    def _get_zone_type(self, zone_id: str) -> ZoneType:
        return self.zones.id_to_type[zone_id]

    # --- state update methods ---

    def _update_from_lifetime_events(self,
                                     *, frame_ts: float,
                                     events: List[LifetimeEvent]) -> None:
        for e in events: # type: LifetimeEvent
            if e.boundary_type == EventBoundaryType.START:
                self._start_vehicle_lifetime(frame_ts=frame_ts,
                                             vehicle_type=e.vehicle_type,
                                             vehicle_id=e.vehicle_id)
            else:
                self._end_vehicle_lifetime(e.vehicle_id)

    def _update_from_zone_events(self,
                                 *, frame_ts: float,
                                 events: List[ZoneOccupancyEvent]) -> None:
        for e in events: # type: ZoneOccupancyEvent
            if e.boundary_type == EventBoundaryType.START:
                self._register_zone_entrance(frame_ts=frame_ts,
                                             vehicle_id=e.vehicle_id,
                                             zone_id=e.zone_id)
            else:
                self._register_zone_exit(frame_ts=frame_ts,
                                         vehicle_id=e.vehicle_id,
                                         zone_id=e.zone_id)

    def _update_from_speed_events(self,
                                  *, frame_ts: float,
                                  events: List[SpeedUpdateEvent]) -> None:
        for e in events:  # type: SpeedUpdateEvent
            self._update_vehicle_speeds(frame_ts=frame_ts,
                                        vehicle_id=e.vehicle_id,
                                        speed_raw=e.speeds.raw,
                                        speed_smoothed=e.speeds.smoothed,
                                        is_matched=e.is_matched)

    # --- state export methods ---

    def _export_state(self) -> LiveAnalyticsState:
        raise NotImplementedError()

    # --- master method ---

    def _check_frame_ts(self, cur_ts: float) -> None:
        prev_ts: float | None = self._prev_frame_ts
        if prev_ts is not None:
            if not cur_ts > prev_ts:
                raise ValueError("The current frame timestamp must be greater than the previous one "
                                 f"(previous {prev_ts}, received current: {cur_ts})")

    def update_and_export_state(self, events_container: EventsContainer) -> LiveAnalyticsState:
        prev_frame_ts: float | None = self._cur_frame_ts

        # timestamp stored as POSIX float, UTC time
        cur_frame_ts: float = datetime_to_utc_posix(events_container.frame_ts)
        self._check_frame_ts(cur_frame_ts)

        self._prev_frame_ts = prev_frame_ts
        self._cur_frame_ts = cur_frame_ts
        self._cur_frame_id = events_container.frame_id

        # NOTE: order DOES matter
        # (1) lifetime
        self._update_from_lifetime_events(frame_ts=cur_frame_ts,
                                          events=events_container.events.lifetime)
        # (2) zone enters / exits
        self._update_from_zone_events(frame_ts=cur_frame_ts,
                                      events=events_container.events.zone_occupancy)
        # (3) speeds
        self._update_from_speed_events(frame_ts=cur_frame_ts,
                                       events=events_container.events.speeds)

        exported_state: LiveAnalyticsState = self._export_state()
        return exported_state