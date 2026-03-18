from typing import override

from tram_analytics.v2.pipeline._base.models.message import WorkerInputMessageWrapper, WorkerOutputMessageWrapper
from tram_analytics.v2.pipeline._mock._base.service.pipeline_stage.adapters.output_adapters.mq.to_workers import (
    StageMQToWorkersOutputPort
)
from tram_analytics.v2.pipeline._mock.common.dto.messages import FrameJobInProgressMessage
from tram_analytics.v2.pipeline._mock.mq.asyncio_broker.brokers.stage_to_worker_server.broker import (
    StageToWorkerServerAsyncioMQBroker
)


class SquarerStageAsyncioBrokerMQToWorkersAdapter(StageMQToWorkersOutputPort):

    def __init__(self, broker: StageToWorkerServerAsyncioMQBroker):
        self._broker: StageToWorkerServerAsyncioMQBroker = broker

    @override
    async def send_to_worker(self, msg: WorkerInputMessageWrapper[FrameJobInProgressMessage]) -> None:
        # serialise if needed per the protocol used
        await self._broker.stage_to_workers.put(msg)

    @override
    async def get_next_message_from_worker(self) -> WorkerOutputMessageWrapper[FrameJobInProgressMessage]:
        return await self._broker.workers_to_stage.get()