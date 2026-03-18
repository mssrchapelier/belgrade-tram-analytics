from typing import override

from tram_analytics.v2.pipeline._base.ingestion_stage.adapters.output_adapters.repository.write_repo import \
    BaseIngestionStageWriteRepo
from tram_analytics.v2.pipeline._mock.common.dto.data_models import EmittedNumberAsStored
from tram_analytics.v2.pipeline._mock.number_emitter.repository.dict_store.mock_dict_store import \
    EmittedNumberMockDictStore


class NumberEmitterWriteMockDictStore(
    BaseIngestionStageWriteRepo[EmittedNumberAsStored]
):

    def __init__(self, dest_persistence: EmittedNumberMockDictStore):
        self._dest_store: EmittedNumberMockDictStore = dest_persistence

    @override
    async def store(self, *, output: EmittedNumberAsStored, timeout: float | None) -> None:
        await self._dest_store.store(output)

    @override
    async def get_new_session_id(self) -> int:
        return self._dest_store.get_next_session_id()