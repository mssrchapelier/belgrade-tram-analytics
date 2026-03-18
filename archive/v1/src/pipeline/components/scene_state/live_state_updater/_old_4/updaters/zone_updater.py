from typing import Set, NamedTuple, List, Dict, override, Iterable, OrderedDict, Iterator
from abc import ABC, abstractmethod
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime

from common.utils.time_utils import posix_to_utc_datetime
from tram_analytics.v1.models.components.scene_state.live_state.zones import CurrentExistenceInZonePeriod, \
    CompletedExistenceInZonePeriod, PreviousAndCurrentExistenceInZonePeriodContainer, LastCompletedByVehiclePeriod, \
    PeriodsForZone


# Functionality for updating a single zone's state based on consumed events.

# --- common base updater ---

class BaseInZoneVehiclePeriodsUpdater(ABC):

    @abstractmethod
    def update_with_vehicles(self, *, event_ts: float, to_add: Set[str], to_remove: Set[str]) -> None:
        pass

# --- periods whose start and end are tied to a SINGLE vehicle ---

class PreviousInZoneSingleVehicleBasedPeriod(NamedTuple):
    # Used to track, for any single vehicle:
    # (1) completed stationary periods ("last tram stopped from ... to ...");
    # (2) completed zone passes for any single vehicle
    #     ("last car that exited this zone entered at ... and exited at ...")

    start_timestamps: List[float]
    end_ts: float

class InZoneSingleVehicleBasedPeriodsUpdater(BaseInZoneVehiclePeriodsUpdater):

    """
    Used for tracking:
    (1) individual stops in the zone;
    (2) individual passes of the zone.

    --- 1 ---
    As used for stops:

    Tracks the start and end timestamp for the last completed period
    during which any single vehicle was stationary in this zone.
    In other words:
    (1) When the last time was that a vehicle that had been stationary
        ceased to be stationary (e. g. in case of tram platforms,
        roughly: when the last tram that has departed, departed;
        more exactly: when the last tram that ceased to be stationary, did that).
    (2) When the respective stationary period started (i. e. in case of trams,
        for how long the tram was stationary at the platform).

    If more than one vehicle became moving at the same time,
    stores the common end timestamp and the start timestamps for all such vehicles.

    --- 2 ---
    Similarly for passes (the period during which any single vehicle was present in this zone).
    """

    def __init__(self) -> None:
        self._prev_completed_period_times: PreviousInZoneSingleVehicleBasedPeriod | None = None

        # vehicle id -> stationary start ts
        self._vehicle_id_to_period_start: Dict[str, float] = dict()

    def _check_before_adding(self, vehicle_ids: Set[str]):
        if any(v_id in self._vehicle_id_to_period_start for v_id in vehicle_ids):
            raise ValueError(f"Can't add vehicles: at least one is already registered in this zone")

    def _add_vehicles(self, *, vehicle_ids: Set[str], event_ts: float) -> None:
        self._check_before_adding(vehicle_ids)
        for vehicle_id in vehicle_ids:
            self._vehicle_id_to_period_start[vehicle_id] = event_ts

    def _check_before_removal(self, *, vehicle_ids: Set[str], event_ts: float) -> None:
        if self._prev_completed_period_times is not None and not event_ts > self._prev_completed_period_times.end_ts:
            raise ValueError("Got period end that is not later than the recorded end of the previous period")
        for vehicle_id in vehicle_ids:
            if vehicle_id not in self._vehicle_id_to_period_start:
                raise ValueError(f"Can't remove vehicle {vehicle_id}: not registered in this zone")
            cur_period_start_ts: float = self._vehicle_id_to_period_start[vehicle_id]
            if event_ts < cur_period_start_ts:
                raise ValueError(
                    "Got period end that is earlier than the period's recorded start: "
                    f"vehicle {vehicle_id}, start {cur_period_start_ts}, got end {event_ts}"
                )

    def _remove_vehicles(self, *, vehicle_ids: Set[str], event_ts: float) -> None:
        self._check_before_removal(vehicle_ids=vehicle_ids, event_ts=event_ts)
        # validated successfully -- remove the vehicles from mappings
        # and get the start timestamps for their respective periods in the zone
        start_timestamps_for_removed: List[float] = [
            self._vehicle_id_to_period_start.pop(vehicle_id)
            for vehicle_id in vehicle_ids
        ]
        # sort in ascending order (remove this if returning with vehicle ids is ever needed in the future)
        start_timestamps_for_removed.sort()
        self._prev_completed_period_times = PreviousInZoneSingleVehicleBasedPeriod(
            start_timestamps=start_timestamps_for_removed,
            end_ts=event_ts
        )

    def _remove_vehicles_new(self) -> None:
        pass

    @override
    def update_with_vehicles(self, *, event_ts: float, to_add: Set[str], to_remove: Set[str]) -> None:
        self._add_vehicles(vehicle_ids=to_add, event_ts=event_ts)
        self._remove_vehicles(vehicle_ids=to_remove, event_ts=event_ts)

    def export_state(self) -> LastCompletedByVehiclePeriod:
        # export data for the last completed period
        start_timestamps: List[datetime] | None = (
            [
                posix_to_utc_datetime(timestamp)
                for timestamp in self._prev_completed_period_times.start_timestamps
            ]
            if self._prev_completed_period_times is not None
            else None
        )
        end_ts: datetime | None = (
            posix_to_utc_datetime(self._prev_completed_period_times.end_ts)
            if self._prev_completed_period_times is not None
            else None
        )

        return LastCompletedByVehiclePeriod(start_timestamps=start_timestamps,
                                            end_ts=end_ts)


# --- periods whose start and end are tied to the EXISTENCE OF AT LEAST ONE vehicle ---

class InZoneExistenceBasedPeriodEndedException(Exception):
    pass

class VehicleIdUpdater:

    """
    A convenience wrapper around an instance of `OrderedDict` storing the current vehicle IDs.
    The current vehicle IDs are stored in the order of their insertion,
    and alphabetically ordered where the time of insertion was the same.
    Defines a property (`vehicle_id_iterator`) that returns an iterator
    over the current vehicle IDs, in the order described.
    """

    def __init__(self) -> None:
        # { vehicle_id: index_at_insertion }
        self._id_to_insertion_timestamp: OrderedDict[str, float] = OrderedDict()

    def _add_vehicle_ids(self, *, vehicle_ids: Iterable[str], cur_ts: float) -> None:
        # convert to list if not a list
        ids_as_list: List[str] = vehicle_ids if isinstance(vehicle_ids, list) else list(vehicle_ids)
        # sort in alphabetical order
        ids_as_list.sort()
        # update current mappings
        self._id_to_insertion_timestamp.update({v_id: cur_ts
                                                for v_id in ids_as_list})

    def _remove_vehicle_ids(self, vehicle_ids: Iterable[str]) -> None:
        for v_id in vehicle_ids: # type: str
            self._id_to_insertion_timestamp.pop(v_id)

    def update(self, *, cur_ts: float, to_add: Iterable[str], to_remove: Iterable[str]) -> None:
        if any(v_id in self._id_to_insertion_timestamp for v_id in to_add):
            raise ValueError("Cannot add vehicle_ids: at least one of the items is already present in the keys")
        if any(v_id not in self._id_to_insertion_timestamp for v_id in to_remove):
            raise ValueError("Cannot remove vehicle_ids: at least one of the items is not present in the keys")
        self._add_vehicle_ids(vehicle_ids=to_add, cur_ts=cur_ts)
        self._remove_vehicle_ids(vehicle_ids=to_remove)


    @property
    def vehicle_id_iterator(self) -> Iterator[str]:
        """
        Returns an iterator over the current vehicle IDs, sorted by insertion order,
        and alphabetically where inserted at the same time.
        """
        # NOTE: "alphabetically where inserted at the same time" -- implemented in `add_vehicle_ids`, not here
        return iter(self._id_to_insertion_timestamp.keys())

    def __len__(self) -> int:
        return len(self._id_to_insertion_timestamp)

class CompletedInZoneExistenceBasedPeriodStats(NamedTuple):
    start_ts: float
    end_ts: float
    max_vehicles: int

# NOTE: not frozen -- meant to be mutable
@dataclass(slots=True, kw_only=True)
class OngoingInZoneExistenceBasedPeriodStats:
    # the start timestamp
    start_ts: float
    # the maximum number of vehicles that were current at any given time during this period
    max_vehicles: int

class InZoneExistenceBasedPeriodsUpdater(BaseInZoneVehiclePeriodsUpdater):

    """
    For the given zone, tracks data regarding the type of in-zone existence period in question:
    (1) the currently ongoing such period, if any;
    (2) the last such completed period, if any.
    "In-zone existence period": a period defined by the presence
    of at least one vehicle in the zone that satisfies certain criteria.
    The period starts when there is at least one such vehicle for the first time,
    and ends when there is no more such vehicles.
    Used for tracking:
    (1) zone occupancy (i. e. periods when there is at least one vehicle in the zone);
    (2) periods when there is at least one vehicle stopped
        (useful for measuring traffic congestion in the zone).
    """

    def __init__(self) -> None:

        self._last_completed_period: CompletedInZoneExistenceBasedPeriodStats | None = None
        self._current_period: OngoingInZoneExistenceBasedPeriodStats | None = None

        # the IDs of current vehicles (and the timestamps when they were registered)
        self._vehicle_id_updater: VehicleIdUpdater = VehicleIdUpdater()
        # the number of current vehicles
        self._cur_vehicles: int = len(self._vehicle_id_updater)

    @override
    def update_with_vehicles(self, *, event_ts: float, to_add: Set[str], to_remove: Set[str]) -> None:
        self._vehicle_id_updater.update(cur_ts=event_ts, to_add=to_add, to_remove=to_remove)
        self._cur_vehicles = len(self._vehicle_id_updater)

        if self._current_period is None and self._cur_vehicles > 0:
            # start the new current period
            self._current_period = OngoingInZoneExistenceBasedPeriodStats(start_ts=event_ts,
                                                                          max_vehicles=self._cur_vehicles)
        elif self._current_period is not None:
            # there is an ongoing period
            if self._cur_vehicles > 0:
                # it is still ongoing; update its max vehicles
                self._current_period.max_vehicles = max(self._cur_vehicles,
                                                        self._current_period.max_vehicles)
            else:
                # it has ended; update the previous period and reset the current one
                self._last_completed_period = CompletedInZoneExistenceBasedPeriodStats(
                    start_ts=self._current_period.start_ts,
                    end_ts=event_ts,
                    max_vehicles=self._current_period.max_vehicles
                )
                self._current_period = None


    def _export_current_period(self) -> CurrentExistenceInZonePeriod:
        start_ts: datetime | None = (posix_to_utc_datetime(self._current_period.start_ts)
                                     if self._current_period is not None
                                     else None)
        max_vehicles: int | None = (self._current_period.max_vehicles
                                    if self._current_period is not None
                                    else None)
        num_vehicles: int = self._cur_vehicles
        vehicle_ids: List[str] = list(self._vehicle_id_updater.vehicle_id_iterator)
        return CurrentExistenceInZonePeriod(start_ts=start_ts,
                                            max_vehicles=max_vehicles,
                                            num_vehicles=num_vehicles,
                                            vehicle_ids=vehicle_ids)

    def _export_completed_period(self) -> CompletedExistenceInZonePeriod:
        start_ts: datetime | None = (posix_to_utc_datetime(self._last_completed_period.start_ts)
                                     if self._last_completed_period is not None
                                     else None)
        end_ts: datetime | None = (posix_to_utc_datetime(self._last_completed_period.end_ts)
                                   if self._last_completed_period is not None
                                   else None)
        max_vehicles: int | None = (self._last_completed_period.max_vehicles
                                    if self._last_completed_period is not None
                                    else None)
        return CompletedExistenceInZonePeriod(start_ts=start_ts,
                                              end_ts=end_ts,
                                              max_vehicles=max_vehicles)

    def export_state(self) -> PreviousAndCurrentExistenceInZonePeriodContainer:
        current: CurrentExistenceInZonePeriod = self._export_current_period()
        previous: CompletedExistenceInZonePeriod = self._export_completed_period()
        return PreviousAndCurrentExistenceInZonePeriodContainer(
            current_period=current, last_completed_period=previous
        )


# --- a coupling for updaters of each of these types ---
# (1) occupancy periods tracker
#     (occupancy by at least one;
#      occupancy by each one, i. e. zone passes)
# (2) in-zone stationary periods tracker
#     (at least one stationary in the zone;
#      each single one stationary in the zone, i. e. stops in the zone)

# base

class SingleEventTypePeriodUpdaters:

    """
    Wraps updaters meant to process a single event type:
    - zone entrance/exit events;
    - in-zone stationary start/end events.
    """

    def __init__(self) -> None:
        self._in_zone_existence_state: InZoneExistenceBasedPeriodsUpdater = InZoneExistenceBasedPeriodsUpdater()
        self._single_vehicle_based_state: InZoneSingleVehicleBasedPeriodsUpdater = InZoneSingleVehicleBasedPeriodsUpdater()

    def update_with_vehicles(self, *, event_ts: float, to_add: Set[str], to_remove: Set[str]) -> None:
        self._in_zone_existence_state.update_with_vehicles(event_ts=event_ts, to_add=to_add, to_remove=to_remove)
        self._single_vehicle_based_state.update_with_vehicles(event_ts=event_ts, to_add=to_add, to_remove=to_remove)

    def export_state(self) -> PeriodsForZone:
        in_zone_existence: PreviousAndCurrentExistenceInZonePeriodContainer = (
            self._in_zone_existence_state.export_state()
        )
        single_vehicle_based: LastCompletedByVehiclePeriod = (
            self._single_vehicle_based_state.export_state()
        )
        return PeriodsForZone(by_at_least_one_vehicle=in_zone_existence,
                              last_completed_by_vehicle=single_vehicle_based)

# --- master updater (for a single zone) ---

@dataclass(frozen=True, slots=True, kw_only=True)
class ZoneState:
    """
    `occupancy`:
    - `_in_zone_existence_state`: continuous occupance
        Tracks whether the zone is occupied, and by how many vehicles;
        stores and updates the start of the current period with at least one vehicle present,
        and the start and end of the last completed such period.
    - `_single_vehicle_based_state`: vehicle passes
        Tracks for how long each vehicle currently present in the zone has been present there.
        Stores and updates the start timestamp (timestamps, if triggered by more than one vehicle)
        and the end timestamp for the last vehicle (vehicles) that exited this zone.
    `stationary_in_zone`:
    - `_in_zone_existence_state`: continuous periods with ststionary vehicles
        Tracks whether there are stationary vehicles in the zone, and how many;
        stores and updates the start of the current period
        with at least stationary vehicle present in the zone,
        and the start and end of the last completed such period.
    - `_single_vehicle_based_state`: vehicle stops
        Tracks for how long each vehicle that is currently stationary in the zone
        has been stationary in this zone.
        Stores and updates the start timestamp (timestamps, if triggered by more than one vehicle)
        and the end timestamp for the last vehicle (vehicles)
        that ceased to be both stationary and present in this zone.
    """

    occupancy: SingleEventTypePeriodUpdaters = field(default_factory=SingleEventTypePeriodUpdaters)
    stationary_in_zone: SingleEventTypePeriodUpdaters = field(default_factory=SingleEventTypePeriodUpdaters)
