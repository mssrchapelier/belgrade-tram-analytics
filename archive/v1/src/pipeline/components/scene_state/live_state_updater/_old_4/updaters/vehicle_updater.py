from typing import Dict, List, override, NamedTuple
from warnings import deprecated
from datetime import datetime

from pydantic import BaseModel

from common.utils.time_utils import posix_to_utc_datetime
from common.utils.builder import BaseBuilder, PropertyAlreadySetException, PropertyNotSetException
from tram_analytics.v1.models.common_types import MotionStatus
from archive.v1.src.pipeline.components.scene_state.live_state_updater._old_4.updaters.speed_updater import (
    SpeedMetricsTracker, SpeedMetrics
)
from tram_analytics.v1.models.components.scene_state.live_state.speeds import LifetimeSpeeds, InZoneSpeeds
from tram_analytics.v1.models.components.scene_state.live_state.vehicles import AgnosticZoneInfoForVehicle, \
    CurrentVehicleStationaryStats, PreviousVehicleStationaryStats, VehicleStationaryStats, MotionStatusContainer, \
    MotionInfoContainer


# Functionality for updating a single vehicle's state based on consumed events.

class VehicleEventData(NamedTuple):
    vehicle_id: str
    event_ts: float

# --- (1) zone-related info for a single vehicle ---

# --- (1.1) for an individual zone ---

class ExportFromVehicleSingleZoneInfo(BaseModel):
    # without zone_id, zone_numerical_id
    present_in_zone_since_ts: datetime
    speed_in_zone_stats: InZoneSpeeds

@deprecated("Deprecated, see details in a comment in class definition")
class Builder_AgnosticZoneRelatedInfoForVehicle(BaseBuilder[AgnosticZoneInfoForVehicle]):

    # NOTE: Implemented but abandoned:
    # this pattern will only warn of an incomplete configuration at runtime
    # and does not allow for static type checking.

    def __init__(self) -> None:
        self._zone_id: str | None = None
        self._zone_numerical_id: int | None = None
        self._present_in_zone_since_ts: datetime | None = None
        self._speed_in_zone_stats: InZoneSpeeds | None = None

    @property
    def zone_id(self) -> str:
        if self._zone_id is None:
            raise PropertyNotSetException()
        return self._zone_id

    @zone_id.setter
    def zone_id(self, value: str) -> None:
        if self._zone_id is not None:
            raise PropertyAlreadySetException()
        self._zone_id = value

    @property
    def zone_numerical_id(self) -> int:
        if self._zone_numerical_id is None:
            raise PropertyNotSetException()
        return self._zone_numerical_id

    @zone_numerical_id.setter
    def zone_numerical_id(self, value: int) -> None:
        if self._zone_numerical_id is not None:
            raise PropertyAlreadySetException()
        self._zone_numerical_id = value

    @property
    def present_in_zone_since_ts(self) -> datetime:
        if self._present_in_zone_since_ts is None:
            raise PropertyNotSetException()
        return self._present_in_zone_since_ts

    @present_in_zone_since_ts.setter
    def present_in_zone_since_ts(self, value: float) -> None:
        if self._present_in_zone_since_ts is not None:
            raise PropertyAlreadySetException()
        self._present_in_zone_since_ts = posix_to_utc_datetime(value)

    @property
    def speed_in_zone_stats(self) -> InZoneSpeeds:
        if self._speed_in_zone_stats is None:
            raise PropertyNotSetException()
        return self._speed_in_zone_stats

    @speed_in_zone_stats.setter
    def speed_in_zone_stats(self, value: InZoneSpeeds) -> None:
        if self._speed_in_zone_stats is not None:
            raise PropertyAlreadySetException()
        self._speed_in_zone_stats = value

    @override
    def build(self) -> AgnosticZoneInfoForVehicle:
        return AgnosticZoneInfoForVehicle(
            zone_id=self.zone_id,
            zone_numerical_id=self.zone_numerical_id,
            present_in_zone_since_ts=self.present_in_zone_since_ts,
            speed_in_zone_stats=self.speed_in_zone_stats
        )

class VehicleSingleZoneInfo:

    """
    For a single zone to which the vehicle is assigned,
    updates speed-based aggregated parameters
    for the duration of the vehicle's current dwell in that zone
    (current -- not meant to be used; max, mean, median).
    """

    def __init__(self, zone_entrance_ts: float) -> None:
        self._zone_entrance_ts: float = zone_entrance_ts
        self._speed_updater: SpeedMetricsTracker = SpeedMetricsTracker()

    @property
    def speeds(self) -> SpeedMetrics | None:
        return self._speed_updater.values

    def update(self, *,
               speed_raw: float | None,
               speed_smoothed: float | None,
               is_matched: bool) -> None:
        self._speed_updater.update(speed_raw=speed_raw,
                                   speed_smoothed=speed_smoothed,
                                   is_matched=is_matched)

    def export_state(self) -> ExportFromVehicleSingleZoneInfo:
        present_in_zone_since_ts: datetime = posix_to_utc_datetime(self._zone_entrance_ts)
        speeds: InZoneSpeeds = self._speed_updater.export_state_as_inzone()
        return ExportFromVehicleSingleZoneInfo(
            present_in_zone_since_ts=present_in_zone_since_ts,
            speed_in_zone_stats=speeds
        )


# --- (1.1) for all zones ---

class ExportFromVehicleZonesInfoItem(ExportFromVehicleSingleZoneInfo):
    # without zone_numerical_id (only available in the master updater)
    zone_id: str

class ExportFromVehicleZonesInfo(BaseModel):
    # mirrors AgnosticZoneRelatedInfosForVehicleContainer,
    # but the items are without without zone_numerical_id (only available in the master updater)
    all_zones: List[ExportFromVehicleZonesInfoItem]

class VehicleZonesInfo:

    """
    Keeps track of the zones to which a single vehicle is assigned.
    For every zone, updates speed-based aggregated parameters
    for the duration of the vehicle's current dwell in that zone
    (current -- not meant to be used; max, mean, median).
    """

    def __init__(self) -> None:
        self._zones: Dict[str, VehicleSingleZoneInfo] = dict()

    def add_zone(self, *, zone_id: str, zone_entrance_ts: float) -> None:
        if zone_id in self._zones:
            raise ValueError(f"Cannot add zone {zone_id}: already exists in the state")
        self._zones[zone_id] = VehicleSingleZoneInfo(zone_entrance_ts)

    def update_speeds_in_registered_zones(self, *, speed_raw: float | None,
                                          speed_smoothed: float | None,
                                          is_matched: bool) -> None:
        for zone in self._zones.values(): # type: VehicleSingleZoneInfo
            zone.update(speed_raw=speed_raw,
                        speed_smoothed=speed_smoothed,
                        is_matched=is_matched)

    def remove_zone(self, zone_id: str) -> None:
        if zone_id not in self._zones:
            raise ValueError(f"Cannot remove zone {zone_id}: does not exist in the state")
        self._zones.pop(zone_id)

    def export_state(self) -> ExportFromVehicleZonesInfo:
        items: List[ExportFromVehicleZonesInfoItem] = []
        for zone_id, zone_info in self._zones.items(): # type: str, VehicleSingleZoneInfo
            export_from_zone: ExportFromVehicleSingleZoneInfo = zone_info.export_state()
            exported_item: ExportFromVehicleZonesInfoItem = ExportFromVehicleZonesInfoItem(
                zone_id=zone_id,
                present_in_zone_since_ts=export_from_zone.present_in_zone_since_ts,
                speed_in_zone_stats=export_from_zone.speed_in_zone_stats
            )
            items.append(exported_item)
        return ExportFromVehicleZonesInfo(all_zones=items)

# --- (2) stationary periods ---

class PreviousStationaryTimes(NamedTuple):
    start: float
    end: float

class VehicleMotionState(NamedTuple):
    momentary: MotionStatus
    confirmed: MotionStatus

class VehicleMotionStatusTracker:

    # tracks:
    # (1) since when this vehicle has been stationary (if stationary)
    # (2) the timestamps for the last recorded stationary period (if any) for this vehicle

    def __init__(self) -> None:
        self._prev_stationary_times: PreviousStationaryTimes | None = None
        self._cur_stationary_start_ts: float | None = None
        # at init time, set status to undefined
        # self._cur_status: MotionStatus = MotionStatus.UNDEFINED

        self._cur_motion_state: VehicleMotionState = VehicleMotionState(momentary=MotionStatus.UNDEFINED,
                                                                        confirmed=MotionStatus.UNDEFINED)

    def update_motion_status(self, *, momentary: MotionStatus, confirmed: MotionStatus) -> None:
        if (
                momentary is MotionStatus.STATIONARY and confirmed is MotionStatus.MOVING
                or momentary is MotionStatus.MOVING and confirmed is MotionStatus.STATIONARY
        ):
            raise ValueError("Got an impossible combination of global motion statuses: "
                             f"momentary {momentary}, confirmed {confirmed}")
        self._cur_motion_state = VehicleMotionState(momentary=momentary,
                                                    confirmed=confirmed)

    def start_confirmed_stationary(self, event_ts: float) -> None:
        # if self._cur_motion_state.confirmed is MotionStatus.STATIONARY:
        #     raise RuntimeError("Vehicle received global confirmed stationary start event but this vehicle's "
        #                        "confirmed status is already stationary")
        if self._cur_stationary_start_ts is not None:
            raise RuntimeError("Vehicle received global confirmed stationary start event but this vehicle's "
                               "_cur_stationary_start_ts is defined")
        # self._cur_status = MotionStatus.STATIONARY
        self._cur_stationary_start_ts = event_ts

    def end_confirmed_stationary(self, event_ts: float) -> None:
        # if self._cur_status is not MotionStatus.STATIONARY:
        #     raise RuntimeError("Received stationary end event but this vehicle's "
        #                        "lifetime-bound stationary status is not STATIONARY")
        if self._cur_stationary_start_ts is None:
            raise RuntimeError("Vehicle received global confirmed stationary end event but this vehicle's "
                               "_cur_stationary_start_ts is undefined")
        # self._cur_status = MotionStatus.MOVING
        self._prev_stationary_times = PreviousStationaryTimes(
            start=self._cur_stationary_start_ts,
            end=event_ts
        )
        self._cur_stationary_start_ts = None

    def _export_prev_state(self) -> PreviousVehicleStationaryStats:
        start_ts: datetime | None = (
            posix_to_utc_datetime(self._prev_stationary_times.start)
            if self._prev_stationary_times is not None
            else None
        )
        end_ts: datetime | None = (
            posix_to_utc_datetime(self._prev_stationary_times.end)
            if self._prev_stationary_times is not None
            else None
        )
        return PreviousVehicleStationaryStats(start_ts=start_ts, end_ts=end_ts)

    def _export_current_state(self) -> CurrentVehicleStationaryStats:
        start_ts: datetime | None = (posix_to_utc_datetime(self._cur_stationary_start_ts)
                                     if self._cur_stationary_start_ts is not None
                                     else None)
        return CurrentVehicleStationaryStats(start_ts=start_ts)

    def export_state(self) -> MotionInfoContainer:
        previous: PreviousVehicleStationaryStats = self._export_prev_state()
        current: CurrentVehicleStationaryStats = self._export_current_state()
        stationary_stats: VehicleStationaryStats = VehicleStationaryStats(current=current, previous=previous)
        motion_status: MotionStatusContainer = MotionStatusContainer(momentary=self._cur_motion_state.momentary,
                                                                     confirmed=self._cur_motion_state.confirmed)
        return MotionInfoContainer(status=motion_status,
                                   stationary_periods=stationary_stats)

# --- (3) master updater (for a single vehicle) ---

class ExportFromVehicleState(BaseModel):
    # mirrors AgnosticVehicle, but without vehicle_id, zone_numerical_id-s
    # (only available in the master updater)
    present_since_ts: datetime
    speed: LifetimeSpeeds
    # stationary: VehicleStationaryStats
    motion: MotionInfoContainer
    zones: ExportFromVehicleZonesInfo

class VehicleState:

    """
    Tracks data for a single vehicle (lifetime, speed stats - global/per-zone, stationary period stats - global).
    """

    def __init__(self,
                 *, lifetime_start_ts: float,
                 # settings: StationaryDeterminationSettings
                 ) -> None:
        # self._settings: StationaryDeterminationSettings = settings

        # stores the start timestamp for the vehicle's lifetime
        self._lifetime_start_ts: float = lifetime_start_ts
        # tracks the current speed and max/mean/median for the vehicle's lifetime
        self._lifetime_speeds_tracker: SpeedMetricsTracker = SpeedMetricsTracker()
        # for all zones to which this vehicle is currently assigned:
        # (1) tracks the (current speed -- duplicated, not meant to be used) and max/mean/median
        #     for the duration of this vehicle's dwell in the zone;
        # (2) stores the start timestamp of the dwell
        self._zone_presence_tracker: VehicleZonesInfo = VehicleZonesInfo()
        # stores the current global motions status (momentary and confirmed)
        # and data for this vehicle's stationary periods (in global terms, i. e. not wrt any specific zone):
        # (1) the start timestamp of the current period (if defined)
        # (2) the start and end timestamps of the previous period (if defined)
        self._motion_status_tracker: VehicleMotionStatusTracker = VehicleMotionStatusTracker()

    def add_zone(self, *, zone_id: str, zone_entrance_ts: float) -> None:
        self._zone_presence_tracker.add_zone(zone_id=zone_id,
                                             zone_entrance_ts=zone_entrance_ts)

    def remove_zone(self, zone_id: str) -> None:
        self._zone_presence_tracker.remove_zone(zone_id)

    def update_speeds(self,
                      *, speed_raw: float | None,
                      speed_smoothed: float | None,
                      is_matched: bool) -> None:
        # NOTE: Does NOT update zone IDs (must be updated in the calling code first)
        self._lifetime_speeds_tracker.update(
            speed_raw=speed_raw, speed_smoothed=speed_smoothed, is_matched=is_matched
        )
        self._zone_presence_tracker.update_speeds_in_registered_zones(
            speed_raw=speed_raw, speed_smoothed=speed_smoothed, is_matched=is_matched
        )

    def update_motion_status(self, *, momentary: MotionStatus, confirmed: MotionStatus) -> None:
        # NOTE: Does NOT start/end motion periods (this is handled by motion period start/end events)
        self._motion_status_tracker.update_motion_status(momentary=momentary, confirmed=confirmed)

    def start_confirmed_stationary(self, event_ts: float) -> None:
        self._motion_status_tracker.start_confirmed_stationary(event_ts)

    def end_confirmed_stationary(self, event_ts: float) -> None:
        self._motion_status_tracker.end_confirmed_stationary(event_ts)

    def export_state(self) -> ExportFromVehicleState:
        present_since_ts: datetime = posix_to_utc_datetime(self._lifetime_start_ts)
        speed: LifetimeSpeeds = self._lifetime_speeds_tracker.export_state_as_lifetime()
        motion: MotionInfoContainer = self._motion_status_tracker.export_state()
        # stationary: VehicleStationaryStats = self._motion_status_tracker.export_state()
        zones: ExportFromVehicleZonesInfo = self._zone_presence_tracker.export_state()

        return ExportFromVehicleState(present_since_ts=present_since_ts,
                                      speed=speed,
                                      # stationary=stationary,
                                      motion=motion,
                                      zones=zones)