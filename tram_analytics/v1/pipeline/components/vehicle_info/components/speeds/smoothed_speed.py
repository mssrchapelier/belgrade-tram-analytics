from abc import abstractmethod, ABC
from typing import override

import numpy as np
from numpy import float64
from numpy.typing import NDArray

from tram_analytics.v1.pipeline.components.vehicle_info.components.speeds.config import (
    SpeedSmoothingMethod, MeanVelocitySpeedSmoothingMethodConfig, SpeedSmoothingMethodConfig
)


class BaseSmoothedSpeedCalculator(ABC):

    @abstractmethod
    def calculate(self,
                  *, positions_in_window: NDArray[float64],
                  times_in_window: NDArray[float64]) -> float | None:
        pass

class MeanVelocitySmoothedSpeedCalculator(BaseSmoothedSpeedCalculator):

    def __init__(self, config: MeanVelocitySpeedSmoothingMethodConfig) -> None:
        self._config: MeanVelocitySpeedSmoothingMethodConfig = config

    @override
    def calculate(self,
                  *, positions_in_window: NDArray[float64],
                  times_in_window: NDArray[float64]) -> float | None:
        total_positions: int = positions_in_window.shape[0]
        if total_positions in (0, 1):
            # no speeds to calculate from zero or one positions
            return None
        # in this implementation, there is no weighting; just take the displacement
        # from the first to the last position and divide it by the window duration
        # shape: (2, )
        displacement_xy: NDArray[float64] = positions_in_window[-1] - positions_in_window[0]
        displacement: float = np.linalg.norm(displacement_xy, axis=0).item()
        time_diff: float = (times_in_window[-1] - times_in_window[0]).item()
        if time_diff <= 0.0:
            # by design, should not happen
            raise RuntimeError("Got a non-positive time difference between two positions")
        speed: float = displacement / time_diff
        return speed


def get_smoothed_speed_calculator(config: SpeedSmoothingMethodConfig) -> BaseSmoothedSpeedCalculator:
    match config.method_name:
        case SpeedSmoothingMethod.MEAN_VELOCITY:
            return MeanVelocitySmoothedSpeedCalculator(config)
        case _:
            raise ValueError(f"No implemented/supported smoothed speed calculator for method {config.method_name}")
