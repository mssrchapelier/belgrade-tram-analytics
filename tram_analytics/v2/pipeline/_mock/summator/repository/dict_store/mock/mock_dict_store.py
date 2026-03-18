from typing import override

from pydantic import BaseModel

from tram_analytics.v2.pipeline._mock._base.repository.dict_repo.mock.mock_dict_store import (
    MockDictStoreDelayConfig, BaseMockDictStore
)
from tram_analytics.v2.pipeline._mock.common.dto.data_models import SumSquares


class SummatorMockDictStoreConfig(BaseModel):
    delay: MockDictStoreDelayConfig

class SummatorMockDictStore(BaseMockDictStore[SumSquares]):

    def __init__(self, config: SummatorMockDictStoreConfig) -> None:
        self._config: SummatorMockDictStoreConfig = config
        super().__init__(delay_config=self._config.delay)

    @override
    @classmethod
    def _extract_frame_id_from_item(cls, item: SumSquares) -> str:
        return item.frame_id