# For speed calculation for trams, the centre proxy reference point is used.
# The current assigned track ID is being stored and updated:
# if it changes to null or a different value, all previously stored speeds/positions are reset.
#
# SPEED SMOOTHING:
# N last positions of the ref point, in world coordinates, are stored
# (for cars, all positions in the ROI; for trams, all positions whilst on the current track).
# The length of the sum vector of these is then taken to be the smoothed speed.
# If the number of stored positions is less than N, the smoothed speed is undefined.

import math
from datetime import datetime
from typing import List, NamedTuple, Collection
from warnings import deprecated

import numpy as np
from numpy import float64, floating, issubdtype
from numpy.typing import NDArray

from common.utils.custom_types import PlanarPosition
from tram_analytics.v1.models.components.vehicle_info import Speeds
from tram_analytics.v1.pipeline.components.vehicle_info.components.speeds.config import SpeedSmoothingWindowConfig, \
    SpeedCalculatorConfig
from tram_analytics.v1.pipeline.components.vehicle_info.components.speeds.smoothed_speed import (
    BaseSmoothedSpeedCalculator, get_smoothed_speed_calculator
)


class TimedPosition(NamedTuple):
    ts: datetime
    position: PlanarPosition

class SpeedCalculator:

    def __init__(self, config: SpeedCalculatorConfig) -> None:
        self._config: SpeedCalculatorConfig = config
        self._smoothed_calculator: BaseSmoothedSpeedCalculator = get_smoothed_speed_calculator(
            self._config.smoothing.method
        )

    @staticmethod
    def _check_inputs_for_speed_calculation(*, positions: NDArray[float64],
                                            times: NDArray[float64]) -> None:
        # shape of positions: (num_positions, 2)
        # shape of times: (num_positions,)
        if not positions.shape[0] == times.shape[0]:
            raise ValueError("positions and times must have the same length")
        are_empty: bool = positions.shape[0] == 0
        if not (issubdtype(positions.dtype, floating)
                and (are_empty
                     or len(positions.shape) == 2 and positions.shape[1] == 2)):
            raise ValueError("positions must be a 2-dimensional floating-point array")
        if not (issubdtype(times.dtype, floating)
                and (are_empty or len(times.shape) == 1)):
            raise ValueError("times must be a 1-dimensional floating-point array")

    @staticmethod
    def _check_times_is_increasing(times: NDArray[float64]) -> None:
        if len(times) > 0:
            # shape of times: (num_positions,)
            is_increasing: bool = np.all(np.diff(times) > 0.0).item()
            if not is_increasing:
                raise ValueError("times must be an increasing sequence")

    def _calculate_raw_speed(self,
                             *, prev_pos: TimedPosition | None,
                             cur_pos: TimedPosition | None) -> float | None:
        """
        Calculate the raw speed as the displacement between the penultimate and the last position
        divided by the time elapsed between the two.

        Pass `None` as `prev_pos` to indicate that there is no previous position available;
        in that case, `None` will be returned.

        If `cur_pos` is `None` (will happen if no rail track has been assigned to this vehicle
        and the reference point selection depends on the presence of track-derived points),
        likewise returns `None`.
        """
        if prev_pos is None or cur_pos is None:
            return None
        displacement: float = math.dist(prev_pos.position, cur_pos.position)
        time_diff: float = cur_pos.ts.timestamp() - prev_pos.ts.timestamp()
        if time_diff <= 0.0:
            raise ValueError("Got a non-positive time difference between two positions")
        speed: float = displacement / time_diff
        return speed

    def _calculate_smoothed_speed(self, history: Collection[TimedPosition]) -> float | None:
        """
        Calculate the smoothed speed based on positions passed as history of those positions
        that are judged to be valid for use as inputs for smoothing
        (the selection of such positions has to be done externally).
        """
        if len(history) < 2:
            return None

        positions_list: List[PlanarPosition] = [item.position for item in history]
        # times in Unix epoch seconds
        pos_times_list: List[float] = [item.ts.timestamp() for item in history]

        # --- get all positions / times ---
        # shape: (num_positions, 2)
        positions: NDArray[float64] = np.array(positions_list, dtype=float64)
        # shape: (num_positions,)
        times: NDArray[float64] = np.array(pos_times_list, dtype=float64)

        self._check_inputs_for_speed_calculation(positions=positions, times=times)
        self._check_times_is_increasing(times)

        window_start_idx: int | None = self._calculate_smoothing_window_start_idx(times)
        if window_start_idx is not None:
            positions_in_window: NDArray[float64] = positions[window_start_idx:]
            times_in_window: NDArray[float64] = times[window_start_idx:]
            smoothed: float | None = self._smoothed_calculator.calculate(
                positions_in_window=positions_in_window,
                times_in_window=times_in_window
            )
            return smoothed
        # could not fit a smoothing window -> return null
        return None

    def calculate_speeds(self,
                         *, prev_pos: TimedPosition | None,
                         cur_pos: TimedPosition | None,
                         smoothing_history: Collection[TimedPosition]) -> Speeds:
        raw: float | None = self._calculate_raw_speed(prev_pos=prev_pos, cur_pos=cur_pos)
        smoothed: float | None = self._calculate_smoothed_speed(smoothing_history)
        speeds: Speeds = Speeds(raw=raw, smoothed=smoothed)
        return speeds

    def _calculate_smoothing_window_start_idx(self, times: NDArray[float64]) -> int | None:
        """
        Given the timestamps corresponding to the part of the history of positional observations
        for the objects that is to be considered for speed smoothing
        (i. e. after trimming, see `_trim_history_for_speed_smoothing`),
        return the index at which the window that ends with the last observation starts
        (so that the duration covered by the resulting window is within the duration bounds
        for the smoothing sliding window specified in the config).

        If there are 0 or 1 positional observations, or fewer observations than needed for the window
        to fit within the specified duration bounds, returns `None`.

        :param times:
          timestamps corresponding to the positions (an `ndarray` of shape `(num_observations, )`),
          in **strictly ascending** order
        :param window_min_duration:
          the minimum desired duration for the smoothing sliding window that ends with the last observation
        :param window_max_duration:
          the maximum desired duration for the smoothing sliding window that ends with the last observation
          (`None` to include all observations into the window)
        :return:
          the index at which the window starts (`None` if unable to fit a window)
    """

        # NOTE: Not validating inputs here to save processing time.

        # shape of positions: (num_positions, 2)
        # shape of times: (num_positions,)

        config: SpeedSmoothingWindowConfig = self._config.smoothing.window

        total_items: int = times.shape[0]
        if total_items in (0, 1):
            # no speeds to calculate from zero or one positions
            return None

        # --- calculate the window based on the MAXIMUM bound ---
        # window end: the last timestamp
        window_end_time: float = times[-1].item()
        window_start_idx: int
        if config.max_duration is None:
            # include the entire provided history into the window --> start index: 0
            window_start_idx = 0
        else:
            # window start (min): makes the window exactly max_window_time_size in size and end at window end
            window_start_min_time: float = window_end_time - config.max_duration
            # window start (actual): the earliest timestamp present that is within the specified window size

            # find the index at which window_start_min_time must be inserted into times;
            # this will be the index with which the window begins
            window_start_idx = np.searchsorted(times, window_start_min_time, side="left").item()
        window_length: int = total_items - window_start_idx
        if window_length <= 0:
            # should not happen by design
            raise RuntimeError("Got an invalid size window (non-positive)")
        if window_length == 1:
            # can't calculate speed inside the window at all because the previous update
            # was earlier than the max_window_time_size (consequently, just one position is inside the window)
            return None

        # --- validate the calculated window based on the MINIMUM bound ---
        window_start_time: float = times[-window_length].item()
        window_duration: float = window_end_time - window_start_time
        if window_duration <= 0.0:
            # should not happen by design
            raise RuntimeError("Got an invalid window duration (non-positive)")
        if window_duration < config.min_duration:
            # The calculated window covers less than the specified minimum bound for the duration.
            # This is expected to happen if there are fewer observations than needed to cover the minimum duration
            # (window_start_idx should be equal to 0).
            # Return null.
            return None

        # checks passed --> return the calculated index
        return window_start_idx
