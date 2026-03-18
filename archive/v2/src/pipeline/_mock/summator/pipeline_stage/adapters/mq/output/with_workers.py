from src.v1_2.pipeline._mock.mq.asyncio_broker.stage_to_worker_server.stage_to_worker_server import StageToWorkerServerAsyncioMQBroker
from src.v1_2.pipeline._mock._base.service.pipeline_stage.adapters.mq.output.with_workers import PipelineStageToWorkersMQBrokerOutputAdapter

# stage <-> workers // output

class SummatorStageToWorkersMQBrokerOutputAdapter(PipelineStageToWorkersMQBrokerOutputAdapter):
    def __init__(self, broker: StageToWorkerServerAsyncioMQBroker) -> None:
        super().__init__(
            queue_to_workers=broker.stage_to_workers,
            queue_from_workers=broker.workers_to_stage
        )