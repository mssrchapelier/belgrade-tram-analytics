from typing import override

from pydantic import BaseModel

from tram_analytics.v2.pipeline._mock._base.repository.dict_repo.mock.mock_dict_store import (
    MockDictStoreDelayConfig, BaseMockDictStore
)
from tram_analytics.v2.pipeline._mock.common.dto.data_models import Square


class SquarerMockDictStoreConfig(BaseModel):
    delay: MockDictStoreDelayConfig

class SquarerMockDictStore(BaseMockDictStore[Square]):

    def __init__(self, config: SquarerMockDictStoreConfig) -> None:
        self._config: SquarerMockDictStoreConfig = config
        super().__init__(delay_config=self._config.delay)

    @override
    @classmethod
    def _extract_frame_id_from_item(cls, item: Square) -> str:
        return item.frame_id