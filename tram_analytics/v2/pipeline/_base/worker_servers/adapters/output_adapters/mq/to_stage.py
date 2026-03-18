from abc import ABC, abstractmethod

from tram_analytics.v2.pipeline._base.models.message import (
    BaseFrameJobInProgressMessage, BaseCriticalErrorMessage,
    WorkerOutputMessageWrapper
)


class BaseWorkerServerMQOutputPort[
    JobInProgressMsgT: BaseFrameJobInProgressMessage,
    CriticalErrMsgT: BaseCriticalErrorMessage
](ABC):

    @abstractmethod
    async def publish_for_completed_job(self, msg: WorkerOutputMessageWrapper[JobInProgressMsgT]) -> None:
        # pass (back to the pipeline stage) a container
        # with either the result of a successful job, or the exception caught (if non-critical)
        pass

    @abstractmethod
    async def report_critical_error(self, msg: CriticalErrMsgT) -> None:
        # send a message about a critical error before raising an exception
        pass