from tram_analytics.v2.pipeline._mock.mq.asyncio_broker.brokers.pipeline_to_stage.broker import PipelineToStagesAsyncioMQBroker
from tram_analytics.v2.pipeline._mock._base.service.pipeline_stage.base_pipeline_stage import PipelineStage
from archive.v2.src.pipeline._mock._base.service.pipeline_stage.adapters.input_adapters._old.asyncio_broker import (
    MockStageAsyncioBrokerInputAdapter_Old
)

# stage <- pipeline // input

class SquarerStageAsyncioBrokerInputAdapter_Old(
    MockStageAsyncioBrokerInputAdapter_Old
):

    def __init__(self, *, broker: PipelineToStagesAsyncioMQBroker,
                 stage: PipelineStage) -> None:
        super().__init__(
            queue_to_stage=broker.in_progress.squarer.to_stage,
            stage=stage
        )
