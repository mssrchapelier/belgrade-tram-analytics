from src.v1_2.pipeline._mock.mq.asyncio_broker.pipeline_to_stages.pipeline_to_stages import PipelineToStagesAsyncioMQBroker
from src.v1_2.pipeline._mock._base.service.pipeline_stage.adapters.mq.output.to_pipeline import StageToPipelineMockMQBrokerOutputAdapter

# stage -> pipeline // output

class SummatorStageToPipelineMockMQBrokerOutputAdapter(StageToPipelineMockMQBrokerOutputAdapter):

    def __init__(self, broker: PipelineToStagesAsyncioMQBroker) -> None:
        super().__init__(queue_to_pipeline=broker.in_progress.summator.from_stage,
                         queue_for_critical=broker.critical,
                         queue_for_dropped=broker.dropped_in_processing)