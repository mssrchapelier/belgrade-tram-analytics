from asyncio import Queue

from tram_analytics.v2.pipeline._base.models.message import WorkerInputMessageWrapper
from tram_analytics.v2.pipeline._mock.common.dto.messages import FrameJobInProgressMessage
from src.v1_2.pipeline._mock.mq.asyncio_broker.stage_to_worker_server.stage_to_worker_server import StageToWorkerServerAsyncioMQBroker
from archive.v2.src.pipeline._mock.summator.worker.worker_server import SummatorWorkerServer
from src.v1_2.pipeline._mock._base.service.worker_server.adapters.mq.input_from_mq.from_asyncio_queue import (
    MockWorkerServerAsyncioInputAdapter_Old
)
from tram_analytics.v2.pipeline._mock.common.dto.data_models import Square, SumSquares

# between the pipeline stage and workers // for the worker, input

class BaseSummatorWorkerServerMQInputAdapterOld(MockWorkerServerAsyncioInputAdapter_Old[Square, SumSquares]):
    pass

class SummatorWorkerServerMockMQBrokerInputAdapter(BaseSummatorWorkerServerMQInputAdapterOld):

    def __init__(self, *, broker: StageToWorkerServerAsyncioMQBroker,
                 worker_server: SummatorWorkerServer) -> None:
        queue_to_consume: Queue[WorkerInputMessageWrapper[FrameJobInProgressMessage]] = broker.stage_to_workers
        super().__init__(queue_to_consume=queue_to_consume, worker_server=worker_server)