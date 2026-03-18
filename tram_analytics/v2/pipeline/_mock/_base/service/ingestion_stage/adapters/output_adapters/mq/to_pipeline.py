from typing import TypeAlias

from tram_analytics.v2.pipeline._base.ingestion_stage.adapters.output_adapters.mq.to_pipeline import (
    BaseIngestionMQToPipeline as BasePort
)
from tram_analytics.v2.pipeline._mock.common.dto.messages import (
    FrameJobInProgressMessage, IngestionDroppedItemMessage, CriticalErrorMessage
)

BaseIngestionMQToPipeline: TypeAlias = BasePort[
    FrameJobInProgressMessage, IngestionDroppedItemMessage, CriticalErrorMessage
]