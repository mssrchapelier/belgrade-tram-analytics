from typing import override

from pydantic import BaseModel

from tram_analytics.v2.pipeline._mock.common.dto.data_models import SumSquares
from src.v1_2.pipeline._mock._base.persistence.dict_persistence._mock.mock_persistence_with_delay import (
    BaseMockDictPersistenceWithDelay, PersistenceMockDelayConfig
)

class SummatorPersistenceConfig(BaseModel):
    delay: PersistenceMockDelayConfig

class RunningSumSquaresPersistence(BaseMockDictPersistenceWithDelay[SumSquares]):

    def __init__(self, config: SummatorPersistenceConfig) -> None:
        self._config: SummatorPersistenceConfig = config
        super().__init__(delay_config=self._config.delay)

    @override
    @classmethod
    def _extract_frame_id_from_item(cls, item: SumSquares) -> str:
        return item.frame_id