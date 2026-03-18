from abc import ABC, abstractmethod

from tram_analytics.v2.pipeline._base.models.message import (
    BaseIngestionDroppedItemMessage, BaseFrameJobInProgressMessage, BaseCriticalErrorMessage
)


class BaseIngestionMQToPipeline[
    JobInProgressMsgT: BaseFrameJobInProgressMessage,
    DroppedItemMsgT: BaseIngestionDroppedItemMessage,
    CriticalErrMsgT: BaseCriticalErrorMessage
](ABC):

    """
    An abstract broker adapter with a channel to publish messages about dropped jobs
    (e. g. for system health monitoring).
    """

    def __init__(self) -> None:
        pass

    @abstractmethod
    async def start(self) -> None:
        # initiate channels, etc.
        pass

    @abstractmethod
    async def stop(self) -> None:
        pass

    @abstractmethod
    async def publish_for_emitted_item(self, msg: JobInProgressMsgT) -> None:
        pass

    @abstractmethod
    async def publish_for_dropped_item(self, msg: DroppedItemMsgT) -> None:
        pass

    @abstractmethod
    async def report_critical_error(self, msg: CriticalErrMsgT) -> None:
        # send a message about a critical error before raising an exception
        pass