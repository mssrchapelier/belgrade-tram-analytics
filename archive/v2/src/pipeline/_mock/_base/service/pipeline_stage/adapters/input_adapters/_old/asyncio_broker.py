import asyncio
import logging
from logging import Logger
from typing import override, Callable, Literal, Set, Any
from asyncio import Queue, Event, Task, Future

from common.utils.logging_utils.logging_utils import get_logger_name_for_object
from tram_analytics.v2.pipeline._base.models.message import MessageWithAckFuture, WorkerJobID

from tram_analytics.v2.pipeline._mock.common.dto.messages import FrameJobInProgressMessage, DroppedJobMessage, CriticalErrorMessage
from tram_analytics.v2.pipeline._base.pipeline_stage.base_pipeline_stage import BasePipelineStage
from archive.v2.src.pipeline._mock.mq.asyncio_broker.brokers.pipeline_to_stage.adapters.stage._old.input_adapter import BasePipelineStageInputAdapter_Old


# stage <- pipeline // input

class MockStageAsyncioBrokerInputAdapter_Old(
    BasePipelineStageInputAdapter_Old[FrameJobInProgressMessage, DroppedJobMessage, CriticalErrorMessage]
):

    def __init__(self, *, queue_to_stage: Queue[FrameJobInProgressMessage],
                 stage: BasePipelineStage[FrameJobInProgressMessage, DroppedJobMessage, CriticalErrorMessage]) -> None:
        super().__init__(pipeline_stage=stage)

        self._queue_handler: AsyncioPipelineToStageBrokerInputMessageHandler_Old[FrameJobInProgressMessage] = (
            AsyncioPipelineToStageBrokerInputMessageHandler_Old(queue_to_consume=queue_to_stage,
                                                                on_receive_func=self.on_receive)
        )

    @override
    async def _after_stage_startup(self) -> None:
        await self._queue_handler.start()

    @override
    async def _before_stage_shutdown(self) -> None:
        await self._queue_handler.shutdown()


class AsyncioPipelineToStageBrokerInputMessageHandler_Old[JobInProgressMsgT: BaseFrameJobInProgressMessage]:

    """
    Given a queue to consume and a function to call on each receive,
    after startup and before stopping, will get items from the queue,
    call the function with the message as the argument,
    and wait for an acknowledgement in a background task.
    A warning will be logged if a negative acknowledgement is received.
    """

    def __init__(
            self, *, queue_to_consume: Queue[JobInProgressMsgT],
            # callback for every item received
            on_receive_func: Callable[[MessageWithAckFuture[JobInProgressMsgT]], None]
    ) -> None:

        self._logger: Logger = logging.getLogger(get_logger_name_for_object(self))
        self._on_receive_func: Callable[
            [MessageWithAckFuture[JobInProgressMsgT]], None
        ] = on_receive_func

        self._queue_to_consume: Queue[JobInProgressMsgT] = queue_to_consume

        self._is_running: bool = False
        self._is_shutting_down_event: Event = Event()
        self._is_shutting_down_event.clear()

        self._pull_from_mq_broker_task: Task[None] | None = None
        self._num_unacknowledged_messages: int = 0
        # shutdown will wait for all messages to be acked/nacked by the worker server
        self._no_unacknowledged_messages_event: Event = Event()
        self._no_unacknowledged_messages_event.set()

    async def start(self) -> None:
        if self._is_running:
            self._logger.warning("This instance is already running")
            return
        if self._is_shutting_down_event.is_set():
            self._logger.warning("Requested startup but this instance is shutting down")
            return
        self._is_running = True
        self._pull_from_mq_broker_task = asyncio.create_task(
            self._loop_pull_from_mq_broker()
        )

    async def shutdown(self) -> None:
        if not self._is_running:
            self._logger.warning("Requested shutdown but is not running")
            return
        if self._is_shutting_down_event.is_set():
            self._logger.warning("This instance is already shutting down")
            return
        self._is_running = False
        self._is_shutting_down_event.set()
        # waiting for messages to stop being pulled from the "broker"'s queue
        assert self._pull_from_mq_broker_task is not None
        await self._pull_from_mq_broker_task
        self._pull_from_mq_broker_task = None
        # waiting for the worker server to complete
        await self._no_unacknowledged_messages_event.wait()
        self._is_shutting_down_event.clear()


    async def _wait_for_acknowledgement(
            self, item_with_ack_future: MessageWithAckFuture[JobInProgressMsgT]
    ) -> None:
        """
        Wait for the acknowledgement of the input message from the worker server,
        or catch an exception and treat as nack (here, simply log).
        """
        job_id: WorkerJobID = item_with_ack_future.message.get_job_id()
        future: Future[None] = item_with_ack_future.ack_future
        try:
            await future
            # acked
        except Exception as exc:
            # nacked
            self._logger.warning(f"Input message for job {job_id} not acknowledged / "
                                 f"message negatively acknowledged")
            pass
        finally:
            self._num_unacknowledged_messages -= 1
            if self._num_unacknowledged_messages == 0:
                self._no_unacknowledged_messages_event.set()

    async def _loop_pull_from_mq_broker(self) -> None:
        self._logger.debug("-- entered _loop_pull_from_mq_broker")
        while True:
            getting_task: Task[JobInProgressMsgT] = asyncio.create_task(
                self._queue_to_consume.get()
            )
            shutdown_signal_task: Task[Literal[True]] = asyncio.create_task(
                self._is_shutting_down_event.wait()
            )
            done, pending = await asyncio.wait(
                {getting_task, shutdown_signal_task},
                return_when=asyncio.FIRST_COMPLETED
            ) # type: Set[Task[Any]], Set[Task[Any]]
            for task in pending: # type: Task[Any]
                task.cancel()
            if shutdown_signal_task in done:
                break
            assert getting_task in done
            item: JobInProgressMsgT = await getting_task
            item_with_ack_future: MessageWithAckFuture[JobInProgressMsgT] = MessageWithAckFuture(message=item)
            # create a task to wait for acknowledgement of this message by the worker server
            ack_task: Task[None] = asyncio.create_task(self._wait_for_acknowledgement(item_with_ack_future))
            self._num_unacknowledged_messages += 1
            self._no_unacknowledged_messages_event.clear()
            # trigger the worker server (send the message to it)
            self._on_receive_func(item_with_ack_future)
