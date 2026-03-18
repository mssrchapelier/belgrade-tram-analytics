from typing import override

from tram_analytics.v2.pipeline._base.worker_servers.adapters.output_adapters.repository.read_repo import \
    BaseWorkerServerReadRepo
from tram_analytics.v2.pipeline._mock.common.dto.data_models import Square
from tram_analytics.v2.pipeline._mock.squarer.repository.dict_store.mock.mock_dict_store import SquarerMockDictStore


class SummatorReadMockDictStore(BaseWorkerServerReadRepo[Square]):

    def __init__(self, src_repo: SquarerMockDictStore):
        self._src_repo: SquarerMockDictStore = src_repo

    @override
    async def retrieve(self, frame_id: str, *, timeout: float | None) -> Square:
        return await self._src_repo.retrieve(frame_id)
