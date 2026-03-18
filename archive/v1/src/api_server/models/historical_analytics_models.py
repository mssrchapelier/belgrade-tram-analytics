from typing import List

from pydantic import BaseModel, Field, NonNegativeFloat, NonNegativeInt

from tram_analytics.v1.pipeline.components.scene_state.config.scene_events_config import SceneEventsConfig

# --- request payload ---

class BaseStopsCountsOptions(BaseModel):
    """
    Settings for vehicle stop events.
    """

    # the minimum duration of the event for it to be included in the event count
    min_duration_to_include_s: NonNegativeFloat = 0.0

class StopsOutsidePlatformsCountsOptions(BaseStopsCountsOptions):
    """
    Settings for tram stop events outside platforms.
    """

    # the maximum duration for an interval between two stop events
    # for them to be counted as a single event in the event count
    merge_intervals_shorter_than_s: NonNegativeFloat = 0.0

class StopsOnPlatformDescriptiveStatsOptions(BaseStopsCountsOptions):
    """
    Settings for tram stop events inside platform zones.
    """

    # whether multiple stops of the same tram on the same platform
    # are counted as a single stop for the event count
    merge_stops_at_same_platform: bool = True

class AnalyticsTramsSettingsWrapper(BaseModel):
    stops_outside_platforms_counts: StopsOutsidePlatformsCountsOptions
    stops_on_platforms_stats: StopsOnPlatformDescriptiveStatsOptions

class AnalyticsSettingsWrapper(BaseModel):
    trams: AnalyticsTramsSettingsWrapper

class APIHistRequestPayload(BaseModel):

    # The ID of the camera for which to request data
    camera_id: str

    # The query period's start and end, as ISO timestrings (UTC).
    # TODO: define behaviour if None is passed (e. g. return analytics for the last hour)
    query_period_start_utc_iso: str | None
    query_period_end_utc_iso: str | None

    # The duration of the period of time by which to bin events.
    # NOTE: The EARLIEST bin will be trimmed as necessary.
    time_binning_period_s: NonNegativeFloat = 900.0 # 15 min

    # Whether to include: (1) ValuesForTimeBin.data; (2) EventTimeline.items.
    # Setting this to `True` will increase the size of the response's payload,
    # but will include per-event data, which can be used e. g. to draw boxplots.
    # Otherwise, just the descriptive statistics for each time bin and for the query period are returned.
    include_event_series: bool = True

    analytics_settings: AnalyticsSettingsWrapper

# --- common data models ---

class BaseTimeBinItem(BaseModel):
    time_bin_start_utc_iso: str
    time_bin_end_utc_iso: str
    # The precomputed duration of this time bin (can differ from `time_binning_period_s` if trimmed).
    time_bin_duration_s: NonNegativeFloat

class DescriptiveStats(BaseModel):
    min: float
    q1: float
    median: float
    q3: float
    max: float

    mean: float
    std: float

    n: NonNegativeInt

# --- time series ---

# --- (1) timelines for events ---

class EventTimelineItem(BaseModel):
    event_start_utc_iso: str
    event_end_utc_iso: str
    event_id: str

class EventTimeline(BaseModel):

    # % include_event_series
    items: List[EventTimelineItem] | None

# Containers for:
# (1) data per time bin;
# (2) descriptive stats per time bin;
# (3) descriptive stats for the entire query period.

# --- (2) event counts ---

class CountForTimeBin(BaseTimeBinItem):
    """
    A container for a floating-point value associated with a single time bin.
    """
    value: NonNegativeInt

class EventCountsForTimeBins(BaseModel):
    # Per time bin: event count for the time bin.
    per_time_bin: List[CountForTimeBin]
    # Descriptive statistics for the entire query period.
    stats_for_query_period: DescriptiveStats

# --- (3) event-value mappings ---

# A single value is associated with every event (dwell duration, max speed attained, etc.) inside the query period.
# All such mappings are associated with time bins (one event may fall into more than one time bin).
# Descriptive statistics are computed:
# (1) for events inside each time bin;
# (2) for the entire query period (with overlaps accounted for).
# All mappings are only returned if `include_event_series` is set to `true`.
# Descriptive statistics for the time bins and for the query period are always returned.

class ValueForEvent(BaseModel):
    # The ID of the event with which the value is associated.
    # Needed to properly discard events that cover more than one time bin
    # when assembling a combined list of events for the entire query period.
    event_id: str
    # The value associated with the event.
    value: float

class ValuesForTimeBin(BaseTimeBinItem):

    # % include_event_series
    data: List[ValueForEvent] | None

    stats: DescriptiveStats

class EventValuesForTimeBins(BaseModel):
    """
    A container for:
    (1) per time bin: data comprising the time bin; descriptive statistics for these data;
    (2) the descriptive statistics for the entire query period
        (with event overlaps accounted for, every event being counted only once).

    NOTE:
        The client can draw a boxplot for a time bin by accessing the data for it.
        To draw a boxplot for the entire query period, the client must assemble the data
        from the time-binned arrays (`include_event_series` must be set to `true`).
        Not passing the assembled data for the entire query period here once more,
        because if they are passed at all, then passing them twice would be wasteful.
    """

    unit: str
    data_per_time_bin: List[ValuesForTimeBin]
    stats_for_query_period: DescriptiveStats

# --- (4) event-duration mappings ---

# For every event inside the query period,
# a timeline item and the event duration are computed and mapped to the event.
# All such mappings are associated with time bins (one event may fall into more than one time bin).
# Descriptive statistics are computed for the durations of events:
# (1) per time bin;
# (2) for the entire query period (with overlaps accounted for).
# All mappings and the timeline are only returned if `include_event_series` is set to `true`.
# Descriptive statistics for the time bins and for the query period are always returned.

# NOTE: An event is included in the time bin / query period
# if either its start timestamp or its end timestamp falls inside it.

class BaseDurations(BaseModel):
    """
    Events of a certain class that occurred inside the query period.
    """

    timeline: EventTimeline

    # The total duration of all events inside the query period:
    # (1) with overlaps merged, AND
    # (2) only the duration inside the query period included
    # (for events that start and/or end outside the query period).
    total_s: NonNegativeFloat
    # The fraction of the query period's duration that `total_s` occupies.
    percentage_of_total_time: float = Field(ge=0.0, le=1.0)
    # Per each time bin:
    # (1) durations of all events falling into that bin;
    # (2) descriptive statistics for these durations.
    durations: EventValuesForTimeBins

class BaseDurationsInZone(BaseDurations):
    """
    Events of a certain class that occurred inside the query period
    """

    # Same as `total_s`, but includes only the duration that is associated with the zone
    # (e. g.: that a vehicle spent inside the zone, not its entire lifetime).
    total_in_zone_s: NonNegativeFloat
    # The fraction of the query period's duration that `total_in_zone_s` occupies.
    percentage_of_in_zone_time: float = Field(ge=0.0, le=1.0)

# class TramStopsOutsidePlatformsDurations(BaseDurationsInZone):
#     pass

# class TramStopsOnPlatformDurations(BaseDurationsInZone):
#     pass

# class IntrusionsDurations(BaseDurations):
#     pass

# class IntrusionsStopsDurations(BaseDurationsInZone):
#     pass

# NOTE regarding max speeds:
# The below stats are computed based on whether the EVENT falls into the time bin / query period.
# If an event starts or ends outside the time bin / query period, it is possible that the maximum speed
# registered for that event was actually attained at a moment outside the time bin / query period.
# The guarantee is only that the entire event does belong to the time bin / query period.

# --- stats for zones ---

class BaseZonePasses(BaseModel):
    counts: EventCountsForTimeBins
    intervals: BaseDurations

# --- (1) track ---

class TrackPasses(BaseZonePasses):
    pass

class TramStopsOutsidePlatformsWrapper(BaseModel):
    counts: EventCountsForTimeBins
    durations: BaseDurationsInZone

# class TrackUtilisationWrapper(BaseDurations):
#     pass

class Track(BaseModel):

    track_id: int
    description: str

    passes: TrackPasses
    stops_outside_platforms: TramStopsOutsidePlatformsWrapper
    utilisation: BaseDurations
    max_speeds: EventValuesForTimeBins

# --- (2) platform ---

# class PlatformHeadways(BaseDurations):
#     pass

class BasePlatformPasses(BaseZonePasses):
    pass

class AllPlatformPasses(BasePlatformPasses):
    pass

class NonStoppingPlatformPasses(BasePlatformPasses):
    pass

class StoppingPlatformPasses(BasePlatformPasses):
    stops_durations: BaseDurationsInZone

class PlatformPassesWrapper(BaseModel):
    all: AllPlatformPasses
    stopping: StoppingPlatformPasses
    non_stopping: NonStoppingPlatformPasses

class Platform(BaseModel):
    platform_id: int
    description: str
    track_id: int

    passes: PlatformPassesWrapper

# --- (3) intrusion zone ---

class IntrusionZone(BaseModel):

    intrusion_zone_id: int
    description: str

    counts: EventCountsForTimeBins
    intrusion_durations: BaseDurations
    stop_durations: BaseDurationsInZone
    max_speeds: EventValuesForTimeBins

# --- response data model ---

class CommonMetadata(BaseModel):

    request: APIHistRequestPayload
    server_settings: SceneEventsConfig

class HistoricalAnalytics(BaseModel):

    api_version: str = "0.1.0"

    metadata: CommonMetadata

    tracks: List[Track]
    platforms: List[Platform]
    intrusions: List[IntrusionZone]