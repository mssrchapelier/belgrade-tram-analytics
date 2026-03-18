from typing import override

from tram_analytics.v2.pipeline._mock.common.dto.data_models import Square, SumSquares
from tram_analytics.v2.pipeline._base.worker_servers.adapters.output_adapters.repository.write_repo import \
    BaseWriteRepo
from tram_analytics.v2.pipeline._base.worker_servers.adapters.output_adapters.repository.read_repo import \
    BaseWorkerServerReadRepo
from tram_analytics.v2.pipeline._mock.squarer.repository.dict_store.mock.mock_dict_store import SquarerMockDictStore
from archive.v2.src.pipeline._mock.summator.persistence import RunningSumSquaresPersistence


class SummatorReadRepositoryOutputPort(BaseWorkerServerReadRepo[Square]):

    def __init__(self, src_persistence: SquarerMockDictStore):
        self._src_persistence: SquarerMockDictStore = src_persistence

    @override
    async def retrieve(self, frame_id: str, *, timeout: float | None) -> Square:
        return await self._src_persistence.retrieve(frame_id)

class SummatorWriteRepositoryOutputPort(BaseWriteRepo[SumSquares]):

    def __init__(self, dest_persistence: RunningSumSquaresPersistence):
        self._dest_persistence: RunningSumSquaresPersistence = dest_persistence

    @override
    async def store(self, *, output: SumSquares, timeout: float | None) -> None:
        await self._dest_persistence.store(output)