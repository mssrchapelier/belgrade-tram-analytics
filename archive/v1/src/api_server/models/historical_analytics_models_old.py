from typing import List, Tuple, TypeAlias
from pydantic import BaseModel, NonNegativeFloat, NonNegativeInt

class SingleMetricDescriptiveStats(BaseModel):
    unit: str

    min: float
    q1: float
    median: float
    q3: float
    max: float

    mean: float
    std: float

    n: NonNegativeInt

# [ ( event_1_start_ts, event_1_end_ts ), ... ] # Unix epoch seconds
Timeline: TypeAlias = List[Tuple[float, float]]

class Plot(BaseModel):
    # [ (x_1, y_1), ..., (x_n, y_n) ]
    points: List[Tuple[float, float]]
    # for time bins: x -> time bin start timestamps, Unix epoch seconds
    # y -> plotted values
    x_unit: str  # for time bins: "epoch_s"
    y_unit: str  # unit name for the plotted values

class EventCountsStats(BaseModel):
    # the number of events inside the query period
    count: NonNegativeInt
    # the number of events inside each of the time bins inside the query period
    counts_per_time_bin: Plot

class DwellFilteringOptions(BaseModel):
    # min and max duration of a dwell event for it to be included
    min_duration_s: NonNegativeFloat | None
    max_duration_s: NonNegativeFloat | None
    # if set: maximum duration of an interval between two dwell events for them to be counted as one
    collapse_max_interval_s: NonNegativeFloat | None

class ContinuousEventStats(BaseModel):
    # the total time that events inside the query period cover
    total_s: NonNegativeFloat
    # the percentage of the query period that events inside it cover
    total_time_percentage: NonNegativeFloat
    # event timeline
    timeline: Timeline | None
    # stats for single event durations
    duration_stats: SingleMetricDescriptiveStats | None

class DwellStats(ContinuousEventStats):
    filtering_options: DwellFilteringOptions

class TrackStationaryOutsidePlatformStats(DwellStats):
    counts: EventCountsStats

class TrackUtilisationStats(BaseModel):
    # the total time that this track was occupied by at least one tram
    total_s: NonNegativeFloat
    # the percentage of query period that this track was occupied by at least one tram
    total_time_percentage: NonNegativeFloat
    # for every time bin inside the query period, the percentage of the time bin
    # that this track was occupied by at least one tram
    utilisation_per_time_bin: Plot
    # track occupancy timeline
    timeline: Timeline | None
    # stats for single occupancy event durations
    duration_stats: SingleMetricDescriptiveStats | None

class SpeedWhenMovingStats(BaseModel):
    # for every vehicle appearance event, maps the event's start time (Unix seconds)
    # to the median speed when moving (i. e. above the moving threshold)
    median_speed_per_event: Plot | None
    # stats for vehicle appearance event median speeds
    stats: SingleMetricDescriptiveStats | None

class TrackStats(BaseModel):
    track_id: int
    description: str
    stationary_outside_platforms: TrackStationaryOutsidePlatformStats
    utilisation: TrackUtilisationStats
    speed_when_moving: SpeedWhenMovingStats

class PlatformPassStatsItem(BaseModel):
    # the number of times a tram passed the platform inside the query period
    count: NonNegativeInt
    # the number of pass events inside each of the time bins inside the query period
    counts_per_time_bin: Plot

class PlatformPassStats(BaseModel):
    all: PlatformPassStatsItem
    # stopping passes: passes where the passing tram was stationary
    # for any continuous period time exceeding the threshold
    stopping: PlatformPassStatsItem
    non_stopping: PlatformPassStatsItem

class PlatformHeadwayStatsItem(BaseModel):
    # headway: the duration between the previous tram left the platform zone
    # and the current tram entered the platform zone

    # maps the headway's start time to its duration
    duration_per_event: Plot | None
    # stats for headway durations
    duration_stats: SingleMetricDescriptiveStats | None

class PlatformHeadwayStats(BaseModel):
    all: PlatformHeadwayStatsItem
    stopping: PlatformHeadwayStatsItem
    non_stopping: PlatformHeadwayStatsItem

class PlatformDwellStats(DwellStats):
    # not adding count-based statistics for platform dwells unlike for outside-platform dwells -- not as helpful
    pass

class PlatformStats(BaseModel):
    platform_id: str
    description: str
    track_id: int
    passes: PlatformPassStats
    headways: PlatformHeadwayStats
    dwell: PlatformDwellStats

class IntrusionCountsStats(EventCountsStats):
    pass

class IntrusionDurationStats(ContinuousEventStats):
    pass

class IntrusionStationaryStats(ContinuousEventStats):
    # (time when at least one car was stationary) / (total time covered by all intrusions);
    # a proxy for estimating traffic congestion
    intrusion_time_percentage: NonNegativeFloat

class IntrusionZoneStats(BaseModel):
    zone_id: int
    zone_name: str
    intrusion_counts: IntrusionCountsStats
    intrusion_durations: IntrusionDurationStats
    stationary_durations: IntrusionStationaryStats
    speed_when_moving: SpeedWhenMovingStats

class HistoricalAnalytics(BaseModel):
    api_version: str = "0.1.0"

    query_period_start_utc_iso: str
    query_period_end_utc_iso: str
    time_binning_period_s: NonNegativeFloat
    # whether to include timelines, median_speed_per_event, duration_per_event
    include_event_series: bool = False
    # apply to the above; null for no truncation, seconds to truncate to otherwise; unclipped to the cutoff for straddling
    event_series_truncate_to_last_s: NonNegativeFloat | None
    # the speed threshold to consider the vehicle to be moving (not stationary)
    is_moving_speed_threshold_kmh: NonNegativeFloat
    # the duration for which a tram must have been stationary at a platform for the pass to count as a stopping pass
    platform_stopping_pass_stationary_for_threshold_s: NonNegativeFloat

    tracks: List[TrackStats]
    platforms: List[PlatformStats]
    intrusion_zones: List[IntrusionZoneStats]