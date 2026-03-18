from src.v1_2.pipeline._mock.mq.asyncio_broker.stage_to_worker_server.stage_to_worker_server import StageToWorkerServerAsyncioMQBroker
from src.v1_2.pipeline._mock._base.service.worker_server.adapters.mq.output_to_mq.to_asyncio_queue import (
    MockWorkerServerAsyncioOutputAdapter
)

# between the pipeline stage and workers // for the worker, output

class SquarerWorkerMockMQBrokerOutputAdapter(MockWorkerServerAsyncioOutputAdapter):

    def __init__(self, broker: StageToWorkerServerAsyncioMQBroker) -> None:
        super().__init__(queue_for_publishing=broker.workers_to_stage,
                         queue_for_critical=broker.critical)
