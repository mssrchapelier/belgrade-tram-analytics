from typing import override
from abc import ABC
from asyncio import Queue

from tram_analytics.v2.pipeline._base.models.message import WorkerInputMessageWrapper
from archive.v2.src.pipeline._base.worker_servers.adapters.input_adapters.asyncio_broker import BaseWorkerServerInputAdapter_Old
from tram_analytics.v2.pipeline._base.worker_servers.base_worker_server import BaseWorkerServerConfig, BaseWorkerServer
from tram_analytics.v2.pipeline._mock.common.dto.messages import FrameJobInProgressMessage, CriticalErrorMessage
from src.v1_2.pipeline._mock.mq.asyncio_broker.stage_to_worker_server.input_adapter import (
    AsyncioPipelineStageToWorkerServerBrokerInputMessageHandler_Old
)

# between the pipeline stage and workers // for the worker, input

class MockWorkerServerAsyncioInputAdapter_Old[InputT, OutputT](
    BaseWorkerServerInputAdapter_Old[
        FrameJobInProgressMessage, FrameJobInProgressMessage, CriticalErrorMessage, InputT, OutputT, BaseWorkerServerConfig
    ],
    ABC
):

    """
    An input adapter for the worker server that consumes messages
    from an asyncio queue-based broker (in-process, in-thread).
    Used for communication between the worker server
    and either the stage or the main orchestration component
    where the broker is lightweight and runs in the same process,
    to avoid the need for an external broker in this case.
    """

    def __init__(self,
                 worker_server: BaseWorkerServer[
                     FrameJobInProgressMessage, FrameJobInProgressMessage, CriticalErrorMessage,
                     InputT, OutputT, BaseWorkerServerConfig
                 ],
                 queue_to_consume: Queue[WorkerInputMessageWrapper[FrameJobInProgressMessage]]) -> None:
        super().__init__(worker_server=worker_server)
        self._queue_handler: AsyncioPipelineStageToWorkerServerBrokerInputMessageHandler_Old[FrameJobInProgressMessage] = (
            AsyncioPipelineStageToWorkerServerBrokerInputMessageHandler_Old(queue_to_consume=queue_to_consume,
                                                                            on_receive_func=self.on_receive)
        )

    @override
    async def _after_worker_server_startup(self) -> None:
        await self._queue_handler.start()

    @override
    async def _before_worker_server_shutdown(self) -> None:
        await self._queue_handler.shutdown()
