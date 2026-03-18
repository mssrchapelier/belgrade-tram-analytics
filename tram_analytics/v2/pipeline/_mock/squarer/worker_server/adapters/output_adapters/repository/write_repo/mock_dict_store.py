from typing import override

from tram_analytics.v2.pipeline._base.worker_servers.adapters.output_adapters.repository.write_repo import \
    BaseWriteRepo
from tram_analytics.v2.pipeline._mock.common.dto.data_models import Square
from tram_analytics.v2.pipeline._mock.squarer.repository.dict_store.mock.mock_dict_store import SquarerMockDictStore


class SquarerWriteMockDictStore(BaseWriteRepo[Square]):

    def __init__(self, dest_repo: SquarerMockDictStore):
        self._dest_repo: SquarerMockDictStore = dest_repo

    @override
    async def store(self, *, output: Square, timeout: float | None) -> None:
        await self._dest_repo.store(output)
