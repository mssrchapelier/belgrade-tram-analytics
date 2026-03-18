import asyncio
import time
from datetime import datetime
from typing import AsyncIterator

from numpy.random import Generator, default_rng

from common.utils.time_utils import get_datetime_utc
from tram_analytics.v2.pipeline._mock.common.dto.data_models import NumberObservation


async def mock_num_observation_generator(
    *, num_to_gen: int, bound_min: int, bound_max: int, seed: int,
    simulated_start_ts_iso_str: str, # "2026-01-01T00:00:00.0Z"
    to_sleep: float
) -> AsyncIterator[NumberObservation]:
    rng: Generator = default_rng(seed)

    # fixed simulated start time
    simulated_start_ts: float = datetime.fromisoformat(simulated_start_ts_iso_str).timestamp()
    actual_start: float = time.perf_counter()

    for seq_num in range(num_to_gen):
        await asyncio.sleep(to_sleep)
        elapsed: float = time.perf_counter() - actual_start
        simulated_frame_ts: float = simulated_start_ts + elapsed
        num: int = rng.integers(low=bound_min, high=bound_max).item()
        yield NumberObservation(frame_ts=simulated_frame_ts, number=num)


def get_mock_input_generator(seed: int) -> AsyncIterator[NumberObservation]:
    mock_input_generator: AsyncIterator[NumberObservation] = mock_num_observation_generator(
        num_to_gen=100, bound_min=1, bound_max=10, to_sleep=0.20, seed=seed,
        # simulated_start_ts_iso_str="2026-01-01T00:00:00.0Z"
        simulated_start_ts_iso_str=get_datetime_utc().isoformat()
    )
    return mock_input_generator
