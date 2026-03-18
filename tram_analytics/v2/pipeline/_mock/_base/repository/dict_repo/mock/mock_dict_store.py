import asyncio
from abc import ABC
from typing import override

from numpy.random import Generator, default_rng
from pydantic import BaseModel

from tram_analytics.v2.pipeline._mock._base.repository.dict_repo.dict_store import BaseDictStore


class MockDictStoreDelayConfig(BaseModel):
    seed: int
    min_delay: float
    max_delay: float

class BaseMockDictStore[StoredT](BaseDictStore[StoredT], ABC):

    def __init__(self, delay_config: MockDictStoreDelayConfig) -> None:
        super().__init__()
        self._delay_config: MockDictStoreDelayConfig = delay_config
        self._rng: Generator = default_rng(self._delay_config.seed)

    async def _sleep_for_random_delay(self) -> None:
        delay: float = self._rng.uniform(low=self._delay_config.min_delay,
                                         high=self._delay_config.max_delay)
        await asyncio.sleep(delay)

    @override
    async def store(self, item: StoredT) -> None:
        await self._sleep_for_random_delay()
        await super().store(item)

    @override
    async def retrieve(self, frame_id: str) -> StoredT:
        await self._sleep_for_random_delay()
        return await super().retrieve(frame_id)