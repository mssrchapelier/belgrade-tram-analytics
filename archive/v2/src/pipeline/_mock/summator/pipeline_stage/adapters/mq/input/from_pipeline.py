from src.v1_2.pipeline._mock.mq.asyncio_broker.pipeline_to_stages.pipeline_to_stages import PipelineToStagesAsyncioMQBroker
from tram_analytics.v2.pipeline._mock._base.service.pipeline_stage.base_pipeline_stage import PipelineStage
from src.v1_2.pipeline._mock._base.service.pipeline_stage.adapters.mq.input.asyncio_broker._old.from_pipeline import MockPipelineStageAsyncioInputAdapter_Old

# stage <- pipeline // input

class SummatorStageMockPipelineStageAsyncioInputAdapter(
    MockPipelineStageAsyncioInputAdapter_Old
):

    def __init__(self, *, broker: PipelineToStagesAsyncioMQBroker,
                 stage: PipelineStage) -> None:
        super().__init__(
            queue_to_stage=broker.in_progress.summator.to_stage,
            stage=stage
        )
