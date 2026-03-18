from asyncio import Future
from typing import override

from aio_pika.abc import AbstractRobustConnection, AbstractIncomingMessage

from tram_analytics.v2.mq.rabbit_mq.clients.base_clients.consumer import (
    BaseExchangeConsumerRabbitMQClient, ExchangeConsumerRabbitMQClientConfig
)
from tram_analytics.v2.pipeline._base.models.message import MessageWithAckFuture
from tram_analytics.v2.pipeline._base.pipeline_stage.base_pipeline_stage import BasePipelineStage
from tram_analytics.v2.pipeline._mock.common.dto.messages import (
    FrameJobInProgressMessage, DroppedJobMessage, CriticalErrorMessage
)
from tram_analytics.v2.pipeline._mock.mq.rabbitmq.base.control_and_status_helper import (
    BaseControlAndStatusInputAdapter
)


# wrapper around the pipeline stage to accept JOB REQUEST messages

class StageJobRequestInputAdapter(BaseExchangeConsumerRabbitMQClient):
    """
    Entry point to a pipeline stage consuming job request messages from RabbitMQ.
    """

    def __init__(self, *, config: ExchangeConsumerRabbitMQClientConfig,
                 connection: AbstractRobustConnection,
                 pipeline_stage: BasePipelineStage[
                     FrameJobInProgressMessage, DroppedJobMessage, CriticalErrorMessage
                 ]) -> None:
        super().__init__(config=config, connection=connection)
        self._pipeline_stage: BasePipelineStage[
            FrameJobInProgressMessage, DroppedJobMessage, CriticalErrorMessage
        ] = pipeline_stage

    @override
    async def _on_startup_before_opening_channel(self) -> None:
        await self._pipeline_stage.start()

    @override
    async def _on_stop_after_closing_channel(self) -> None:
        await self._pipeline_stage.shutdown()

    @override
    async def _on_receive(self, msg: AbstractIncomingMessage) -> None:
        converted: FrameJobInProgressMessage = FrameJobInProgressMessage.model_validate_json(msg.body)
        with_future: MessageWithAckFuture[FrameJobInProgressMessage] = MessageWithAckFuture(message=converted)
        future: Future[None] = with_future.ack_future
        # send for processing
        self._pipeline_stage.on_receive(with_future)
        # await the acknowledgement future
        async with msg.process(ignore_processed=True):
            # will ack on no exception, reject on exception
            await future

# wrapper around the job request wrapper to accept CONTROL messages and publish STATUS messages

class ControlAndStatusInputAdapter(
    BaseControlAndStatusInputAdapter[StageJobRequestInputAdapter]
):

    @override
    def _get_is_running_wrapped_client(
            self, wrapped_client: StageJobRequestInputAdapter
    ) -> bool:
        return wrapped_client._is_running

    @override
    async def _start_wrapped_client(
            self, wrapped_client: StageJobRequestInputAdapter
    ) -> None:
        await wrapped_client.start()

    @override
    async def _stop_wrapped_client(
            self, wrapped_client: StageJobRequestInputAdapter
    ) -> None:
        await wrapped_client.stop()