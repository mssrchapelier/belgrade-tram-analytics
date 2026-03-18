from typing import override

from pydantic import BaseModel

from tram_analytics.v2.pipeline._mock._base.repository.dict_repo.mock.mock_dict_store import \
    MockDictStoreDelayConfig, BaseMockDictStore
from tram_analytics.v2.pipeline._mock.common.dto.data_models import EmittedNumber, EmittedNumberAsStored


class EmittedNumberMockDictStoreConfig(BaseModel):
    delay: MockDictStoreDelayConfig

class EmittedNumberMockDictStore(BaseMockDictStore[EmittedNumberAsStored]):

    def __init__(self, config: EmittedNumberMockDictStoreConfig) -> None:
        self._config: EmittedNumberMockDictStoreConfig = config
        super().__init__(delay_config=self._config.delay)

        self._max_stored_session_id: int | None = None

    @override
    async def store(self, item: EmittedNumberAsStored) -> None:
        await super().store(item)
        cur_session_id: int = item.session_id
        last_session_id: int | None = self._max_stored_session_id
        if last_session_id is None or cur_session_id > last_session_id:
            self._max_stored_session_id = cur_session_id

    async def retrieve_emitted_number(self, frame_id: str) -> EmittedNumber:
        emitted_num_as_stored: EmittedNumberAsStored = self._data[frame_id]
        return EmittedNumber(frame_id=emitted_num_as_stored.frame_id,
                             number=emitted_num_as_stored.number)

    def get_next_session_id(self) -> int:
        if self._max_stored_session_id is None:
            return 1
        return self._max_stored_session_id + 1

    @override
    @classmethod
    def _extract_frame_id_from_item(cls, item: EmittedNumberAsStored) -> str:
        return item.frame_id