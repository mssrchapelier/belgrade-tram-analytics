from typing import override

from tram_analytics.v2.pipeline._base.worker_servers.adapters.output_adapters.repository.read_repo import \
    BaseWorkerServerReadRepo
from tram_analytics.v2.pipeline._mock.common.dto.data_models import EmittedNumber
from tram_analytics.v2.pipeline._mock.number_emitter.repository.dict_store.mock_dict_store import \
    EmittedNumberMockDictStore


class SquarerReadMockDictStore(BaseWorkerServerReadRepo[EmittedNumber]):

    def __init__(self, src_repo: EmittedNumberMockDictStore):
        self._src_repo: EmittedNumberMockDictStore = src_repo

    @override
    async def retrieve(self, frame_id: str, *, timeout: float | None) -> EmittedNumber:
        return await self._src_repo.retrieve_emitted_number(frame_id)
