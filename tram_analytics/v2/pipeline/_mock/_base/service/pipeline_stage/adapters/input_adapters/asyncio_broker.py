from asyncio import Queue
from typing import Callable, override

from tram_analytics.v2.pipeline._base.models.message import (
    MessageWithAckFuture
)
from tram_analytics.v2.pipeline._mock._base.service.pipeline_stage.base_pipeline_stage import PipelineStage, \
    PipelineStageConfig
from tram_analytics.v2.pipeline._mock.common.dto.messages import FrameJobInProgressMessage
from tram_analytics.v2.pipeline._mock.mq.asyncio_broker.base.adapters.base_input_adapter import \
    BaseAsyncioBrokerInputAdapter


class BaseStageInputAdapter[PipelineStageConfigT: PipelineStageConfig](BaseAsyncioBrokerInputAdapter[
    FrameJobInProgressMessage, FrameJobInProgressMessage
]):

    def __init__(self, *,
                 # the messages consumed from the broker are not wrapped
                 queue_to_consume: Queue[FrameJobInProgressMessage],
                 pipeline_stage: PipelineStage[PipelineStageConfigT]):
        super().__init__(queue_to_consume)
        self._pipeline_stage: PipelineStage[PipelineStageConfigT] = pipeline_stage

    @override
    def _get_on_receive_func(self) -> Callable[[MessageWithAckFuture[FrameJobInProgressMessage]], None]:
        return self._pipeline_stage.on_receive

    @override
    async def _on_startup(self) -> None:
        await self._pipeline_stage.start()

    @override
    async def _on_shutdown(self) -> None:
        await self._pipeline_stage.shutdown()

    @override
    def _convert_message_from_broker_to_worker(
            self, from_broker_msg: FrameJobInProgressMessage
    ) -> FrameJobInProgressMessage:
        # not wrapped, just return it
        return from_broker_msg