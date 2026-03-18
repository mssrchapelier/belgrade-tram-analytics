import asyncio
from abc import ABC, abstractmethod
from asyncio import Queue, QueueShutDown, Task

from tram_analytics.v2.pipeline._base.models.message import (
    BaseDroppedJobMessage, BaseFrameJobInProgressMessage, BaseCriticalErrorMessage
)


class BaseOutputMQSingleChannel[OutputMsgT](ABC):

    def __init__(self) -> None:
        # messages ready to be published to the message queue
        self._in_queue: Queue[OutputMsgT] = Queue()

        self._push_to_broker_task: Task[None] = asyncio.create_task(
            self._loop_push_to_broker()
        )

    async def shutdown(self) -> None:
        self._in_queue.shutdown()
        await self._in_queue.join()

    @abstractmethod
    async def _push_message_to_broker(self, item: OutputMsgT) -> None:
        pass

    async def _loop_push_to_broker(self) -> None:
        while True:
            try:
                item: OutputMsgT = await self._in_queue.get()
                await self._push_message_to_broker(item)
            except QueueShutDown:
                break

    def enqueue_for_publishing(self, message: OutputMsgT) -> None:
        """
        Enqueue a message to be published to the message queue broker.
        """
        # No max size, no locks around the queue, so put_nowait should not cause issues.
        self._in_queue.put_nowait(message)

class BaseOutputMQMultiChannel[
    JobInProgressMsgT: BaseFrameJobInProgressMessage,
    DroppedJobMsgT: BaseDroppedJobMessage,
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
    async def publish_for_completed_job(self, msg: JobInProgressMsgT) -> None:
        pass

    @abstractmethod
    async def publish_for_failed_job(self, msg: DroppedJobMsgT) -> None:
        pass

    @abstractmethod
    async def report_critical_error(self, msg: CriticalErrMsgT) -> None:
        # send a message about a critical error before raising an exception
        pass