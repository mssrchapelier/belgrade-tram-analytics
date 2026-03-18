from abc import ABC

from tram_analytics.v2.pipeline._base.ingestion_stage.base_ingestion_stage import (
    BaseIngestionStage as BaseStage, BaseIngestionStageConfig as BaseConfig
)
from tram_analytics.v2.pipeline._mock.common.dto.messages import (
    FrameJobInProgressMessage, IngestionDroppedItemMessage, CriticalErrorMessage
)


class BaseIngestionStageConfig(BaseConfig, ABC):
    pass

class BaseIngestionStage[InputT, OutputT, ConfigT: BaseIngestionStageConfig](
    BaseStage[
        InputT, OutputT, ConfigT,
        FrameJobInProgressMessage, IngestionDroppedItemMessage, CriticalErrorMessage
    ],
    ABC
):
    pass