from typing import override

from tram_analytics.v2.pipeline._base.worker_servers.adapters.output_adapters.repository.write_repo import \
    BaseWriteRepo
from tram_analytics.v2.pipeline._mock.common.dto.data_models import SumSquares
from tram_analytics.v2.pipeline._mock.summator.repository.dict_store.mock.mock_dict_store import \
    SummatorMockDictStore


class SummatorWriteMockDictStore(BaseWriteRepo[SumSquares]):

    def __init__(self, dest_repo: SummatorMockDictStore):
        self._dest_repo: SummatorMockDictStore = dest_repo

    @override
    async def store(self, *, output: SumSquares, timeout: float | None) -> None:
        await self._dest_repo.store(output)
