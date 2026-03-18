from abc import ABC, abstractmethod
from warnings import deprecated

from tram_analytics.v2.pipeline._base.models.message import (
    BaseFrameJobInProgressMessage, MessageWithAckFuture,
    BaseDroppedJobMessage, BaseCriticalErrorMessage
)
from tram_analytics.v2.pipeline._base.pipeline_stage.base_pipeline_stage import (
    BasePipelineStage
)

@deprecated(
    "Deprecated; inject BasePipelineStage instance into the MQ broker-specific adapter instead"
)
class BasePipelineStageInputAdapter_Old[
    JobInProgressMsgT: BaseFrameJobInProgressMessage,
    DroppedJobMsgT: BaseDroppedJobMessage,
    CriticalErrMsgT: BaseCriticalErrorMessage
](ABC):

    def __init__(self, pipeline_stage: BasePipelineStage[
        JobInProgressMsgT, DroppedJobMsgT, CriticalErrMsgT
    ]) -> None:
        self._pipeline_stage: BasePipelineStage[
            JobInProgressMsgT, DroppedJobMsgT, CriticalErrMsgT
        ] = pipeline_stage

    async def start(self) -> None:
        await self._pipeline_stage.start()
        await self._after_stage_startup()

    @abstractmethod
    async def _after_stage_startup(self) -> None:
        pass

    async def shutdown(self) -> None:
        await self._before_stage_shutdown()
        await self._pipeline_stage.shutdown()

    @abstractmethod
    async def _before_stage_shutdown(self) -> None:
        pass

    def on_receive(self, input_msg_with_ack_future: MessageWithAckFuture[JobInProgressMsgT]) -> None:
        self._pipeline_stage.on_receive(input_msg_with_ack_future)