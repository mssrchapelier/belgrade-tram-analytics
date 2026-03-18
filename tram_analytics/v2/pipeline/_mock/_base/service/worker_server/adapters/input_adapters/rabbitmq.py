from asyncio import Future
from typing import override

from aio_pika.abc import (
    AbstractRobustConnection, AbstractIncomingMessage
)

from tram_analytics.v2.mq.rabbit_mq.clients.base_clients.consumer import (
    BaseExchangeConsumerRabbitMQClient, ExchangeConsumerRabbitMQClientConfig
)
from tram_analytics.v2.pipeline._base.models.message import MessageWithAckFuture
from tram_analytics.v2.pipeline._base.worker_servers.base_worker_server import BaseWorkerServerConfig, \
    BaseWorkerServer
from tram_analytics.v2.pipeline._mock.common.dto.messages import (
    FrameJobInProgressMessage, CriticalErrorMessage
)
from tram_analytics.v2.pipeline._mock.mq.rabbitmq.base.control_and_status_helper import (
    BaseControlAndStatusInputAdapter
)


# wrapper around the worker server to accept JOB REQUEST messages

class WorkerServerJobRequestInputAdapter[
    InputT, OutputT, WorkerServerConfigT: BaseWorkerServerConfig
](BaseExchangeConsumerRabbitMQClient):
    """
    Entry point to a worker server consuming job request messages from RabbitMQ.
    """

    def __init__(self, *, config: ExchangeConsumerRabbitMQClientConfig,
                 connection: AbstractRobustConnection,
                 worker_server: BaseWorkerServer[
                     FrameJobInProgressMessage, FrameJobInProgressMessage,
                     CriticalErrorMessage, InputT, OutputT, WorkerServerConfigT
                 ]) -> None:
        super().__init__(config=config, connection=connection)
        self._worker_server: BaseWorkerServer[
            FrameJobInProgressMessage, FrameJobInProgressMessage,
            CriticalErrorMessage, InputT, OutputT, WorkerServerConfigT
        ] = worker_server

    @override
    async def _on_startup_before_opening_channel(self) -> None:
        await self._worker_server.start()

    @override
    async def _on_stop_after_closing_channel(self) -> None:
        await self._worker_server.shutdown()

    @override
    async def _on_receive(self, msg: AbstractIncomingMessage) -> None:
        converted: FrameJobInProgressMessage = FrameJobInProgressMessage.model_validate_json(msg.body)
        with_future: MessageWithAckFuture[FrameJobInProgressMessage] = MessageWithAckFuture(message=converted)
        future: Future[None] = with_future.ack_future
        # send for processing
        self._worker_server.on_receive(with_future)
        # await the acknowledgement future
        async with msg.process(ignore_processed=True):
            # will ack on no exception, reject on exception
            await future

# wrapper around the job request wrapper to accept CONTROL messages and publish STATUS messages

class WorkerServerControlAndStatusInputAdapter[
    InputT, OutputT, WorkerServerConfigT: BaseWorkerServerConfig
](
    BaseControlAndStatusInputAdapter[
        WorkerServerJobRequestInputAdapter[InputT, OutputT, WorkerServerConfigT]
    ]
):

    @override
    def _get_is_running_wrapped_client(
            self, wrapped_client: WorkerServerJobRequestInputAdapter[
                InputT, OutputT, WorkerServerConfigT
            ]
    ) -> bool:
        return wrapped_client._is_running

    @override
    async def _start_wrapped_client(
            self, wrapped_client: WorkerServerJobRequestInputAdapter[
                InputT, OutputT, WorkerServerConfigT
            ]
    ) -> None:
        await wrapped_client.start()

    @override
    async def _stop_wrapped_client(
            self, wrapped_client: WorkerServerJobRequestInputAdapter[
                InputT, OutputT, WorkerServerConfigT
            ]
    ) -> None:
        await wrapped_client.stop()