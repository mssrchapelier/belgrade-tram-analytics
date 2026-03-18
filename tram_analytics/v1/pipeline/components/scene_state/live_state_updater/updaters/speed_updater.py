from abc import ABC, abstractmethod
from typing import NamedTuple, override, List

from common.utils.dynamic_median_updater import DynamicMedianUpdater
from tram_analytics.v1.models.components.scene_state.live_state.speeds import SpeedStats, SpeedStatsWithCurrent, \
    LifetimeSpeeds, \
    InZoneSpeeds


# Functionality for updating a vehicle's (raw and smoothed) current speed
# and statistics thereof (max, mean, median) based on speed update events.
# Used by vehicle state updaters.

# --- container for a single speed type (raw/smoothed) ---

class SpeedMetricsForSpeedType(NamedTuple):
    """
    Stores all current values for one speed type (raw or smoothed).
    """
    current: float | None
    max: float | None
    mean: float | None
    median: float | None

# --- calculators for current/max/mean/median ---

class BaseSpeedMetricUpdater(ABC):

    def __init__(self) -> None:
        self._prev_event_ts: float | None = None

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

    def _check_is_sequential(self, event_ts: float) -> None:
        if self._prev_event_ts is not None and event_ts <= self._prev_event_ts:
            raise ValueError("Got a speed update event with the timestamp that is not strictly later "
                             "than the timestamp of the previous update. "
                             "Non-sequential updates are not yet supported.")

    def update(self, *, speed: float | None, is_matched: bool, event_ts: float) -> float | None:
        """
        Updates this object with the current state's parameters
        and returns the recalculated aggregated parameter's value.

        :param speed: the current speed value (`None` if undefined)
        :param is_matched: whether, for this frame, the vehicle is associated with an actual detection
        :returns: the recalculated value of the computed aggregated parameter
        """
        self._check_is_sequential(event_ts)
        if is_matched:
            self._update_with_matched(speed)
        else:
            self._update_with_unmatched(speed)
        self._prev_event_ts = event_ts
        return self.value

class CurrentSpeedUpdater(BaseSpeedMetricUpdater):

    def __init__(self):
        super().__init__()
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
        super().__init__()
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

        self._max_over_confirmed_lifetime = max(max_over) if len(max_over) > 0 else None

        self._reset_last_unmatched()

class MeanSpeedUpdater(BaseSpeedMetricUpdater):

    # Dynamically updates the mean speed over the vehicle's confirmed lifetime.
    # More efficient than recalculating the mean over the entire history,
    # which, for any given step, is O(n) bound on the length of history at that step;
    # this algorithm is O(1).

    def __init__(self):
        super().__init__()
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
        super().__init__()
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

# --- updater for a single speed type (raw/smoothed) ---

class SpeedMetricsUpdaterForSpeedType:

    def __init__(self) -> None:
        self._updater_current: CurrentSpeedUpdater = CurrentSpeedUpdater()
        self._updater_max: MaxSpeedUpdater = MaxSpeedUpdater()
        self._updater_mean: MeanSpeedUpdater = MeanSpeedUpdater()
        self._updater_median: MedianSpeedUpdater = MedianSpeedUpdater()

        self._updaters: List[BaseSpeedMetricUpdater] = [
            self._updater_current, self._updater_max, self._updater_mean, self._updater_median
        ]

        self._current_values: SpeedMetricsForSpeedType | None = None

    @property
    def values(self) -> SpeedMetricsForSpeedType | None:
        return self._current_values

    def update(self, *, speed: float | None, is_matched: bool, event_ts: float) -> SpeedMetricsForSpeedType:
        for updater in self._updaters: # type: BaseSpeedMetricUpdater
            updater.update(speed=speed, is_matched=is_matched, event_ts=event_ts)
        metrics: SpeedMetricsForSpeedType = SpeedMetricsForSpeedType(current=self._updater_current.value,
                                                                     max=self._updater_max.value,
                                                                     mean=self._updater_mean.value,
                                                                     median=self._updater_median.value)
        self._current_values = metrics
        return metrics

    def export_state_with_current_speed(self) -> SpeedStatsWithCurrent:
        output: SpeedStatsWithCurrent = SpeedStatsWithCurrent(
            current_ms=self._updater_current.value,
            max_ms=self._updater_max.value,
            mean_ms=self._updater_mean.value,
            median_ms=self._updater_median.value
        )
        return output

    def export_state_without_current_speed(self) -> SpeedStats:
        output: SpeedStats = SpeedStats(
            max_ms=self._updater_max.value,
            mean_ms=self._updater_mean.value,
            median_ms=self._updater_median.value
        )
        return output

# --- container for speeds ---

class SpeedMetrics(NamedTuple):
    raw: SpeedMetricsForSpeedType
    smoothed: SpeedMetricsForSpeedType

# --- master updater for speeds ---

class SpeedMetricsTracker:

    def __init__(self) -> None:
        self._updater_raw: SpeedMetricsUpdaterForSpeedType = SpeedMetricsUpdaterForSpeedType()
        self._updater_smoothed: SpeedMetricsUpdaterForSpeedType = SpeedMetricsUpdaterForSpeedType()

        self._current_values: SpeedMetrics | None = None

    @property
    def values(self) -> SpeedMetrics | None:
        return self._current_values

    def update(self,
               *, speed_raw: float | None,
               speed_smoothed: float | None,
               is_matched: bool,
               event_ts: float) -> SpeedMetrics:
        metrics_for_raw: SpeedMetricsForSpeedType = self._updater_raw.update(speed=speed_raw,
                                                                             is_matched=is_matched,
                                                                             event_ts=event_ts)
        metrics_for_smoothed: SpeedMetricsForSpeedType = self._updater_smoothed.update(speed=speed_smoothed,
                                                                                       is_matched=is_matched,
                                                                                       event_ts=event_ts)
        metrics: SpeedMetrics = SpeedMetrics(raw=metrics_for_raw,
                                             smoothed=metrics_for_smoothed)
        self._current_values = metrics
        return metrics

    def export_state_as_lifetime(self) -> LifetimeSpeeds:
        output: LifetimeSpeeds = LifetimeSpeeds(
            raw=self._updater_raw.export_state_with_current_speed(),
            smoothed=self._updater_smoothed.export_state_with_current_speed()
        )
        return output

    def export_state_as_inzone(self) -> InZoneSpeeds:
        output: InZoneSpeeds = InZoneSpeeds(
            raw=self._updater_raw.export_state_without_current_speed(),
            smoothed=self._updater_smoothed.export_state_without_current_speed()
        )
        return output