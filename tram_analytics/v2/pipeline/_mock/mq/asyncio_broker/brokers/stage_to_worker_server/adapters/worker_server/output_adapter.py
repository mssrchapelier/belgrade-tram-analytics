from typing import override

from tram_analytics.v2.pipeline._base.models.message import WorkerOutputMessageWrapper
from tram_analytics.v2.pipeline._mock._base.service.worker_server.adapters.output_adapters.mq.to_stage import \
    BaseWorkerServerMQOutputPort
from tram_analytics.v2.pipeline._mock.common.dto.messages import FrameJobInProgressMessage, CriticalErrorMessage
from tram_analytics.v2.pipeline._mock.mq.asyncio_broker.brokers.stage_to_worker_server.broker import \
    StageToWorkerServerAsyncioMQBroker


class WorkerServerAsyncioBrokerOutputAdapter(BaseWorkerServerMQOutputPort):

    def __init__(self, broker: StageToWorkerServerAsyncioMQBroker) -> None:
        super().__init__()
        self._broker: StageToWorkerServerAsyncioMQBroker = broker

    @override
    async def publish_for_completed_job(
            self, msg: WorkerOutputMessageWrapper[FrameJobInProgressMessage]
    ) -> None:
        await self._broker.workers_to_stage.put(msg)

    @override
    async def report_critical_error(self, msg: CriticalErrorMessage) -> None:
        await self._broker.critical.put(msg)
