import asyncio
import logging
from asyncio import Queue, Event, Task, Future
from logging import Logger
from typing import Callable, Literal, Set, Any, override

from common.utils.logging_utils.logging_utils import get_logger_name_for_object
from tram_analytics.v2.pipeline._base.models.message import (
    WorkerInputMessageWrapper, MessageWithAckFuture, WorkerJobID
)
from tram_analytics.v2.pipeline._base.worker_servers.base_worker_server import BaseWorkerServerConfig, \
    BaseWorkerServer
from tram_analytics.v2.pipeline._mock.common.dto.messages import (
    FrameJobInProgressMessage, CriticalErrorMessage
)
from tram_analytics.v2.pipeline._mock.mq.asyncio_broker.base.adapters.base_input_adapter import \
    BaseAsyncioBrokerInputAdapter


class AsyncioPipelineStageToWorkerServerBrokerInputMessageHandler_Old[JobInProgressMsgT: FrameJobInProgressMessage]:

    """
    Given a queue to consume and a function to call on each receive,
    after startup and before stopping, will get items from the queue,
    call the function with the message as the argument,
    and wait for an acknowledgement in a background task.
    A warning will be logged if a negative acknowledgement is received.
    """

    def __init__(
            self, *, queue_to_consume: Queue[WorkerInputMessageWrapper[JobInProgressMsgT]],
            # callback for every item received
            on_receive_func: Callable[[MessageWithAckFuture[JobInProgressMsgT]], None]
    ) -> None:

        self._logger: Logger = logging.getLogger(get_logger_name_for_object(self))
        self._on_receive_func: Callable[
            [MessageWithAckFuture[JobInProgressMsgT]], None
        ] = on_receive_func

        self._queue_to_consume: Queue[WorkerInputMessageWrapper[JobInProgressMsgT]] = queue_to_consume

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
        while True:
            getting_task: Task[WorkerInputMessageWrapper[JobInProgressMsgT]] = asyncio.create_task(
                self._queue_to_consume.get()
            )
            shutdown_signal_task: Task[Literal[True]] = asyncio.create_task(
                self._is_shutting_down_event.wait()
            )
            done, pending = await asyncio.wait(
                {getting_task, shutdown_signal_task},
                return_when=asyncio.FIRST_COMPLETED
            ) # type: Set[Task[Any]], Set[Task[Any]]
            if shutdown_signal_task in done:
                break
            assert getting_task in done
            item: WorkerInputMessageWrapper[JobInProgressMsgT] = await getting_task
            item_msg: JobInProgressMsgT = item.inputs_msg
            item_with_ack_future: MessageWithAckFuture[JobInProgressMsgT] = MessageWithAckFuture(message=item_msg)
            # create a task to wait for acknowledgement of this message by the worker server
            ack_task: Task[None] = asyncio.create_task(self._wait_for_acknowledgement(item_with_ack_future))
            self._num_unacknowledged_messages += 1
            self._no_unacknowledged_messages_event.clear()
            # trigger the worker server (send the message to it)
            self._on_receive_func(item_with_ack_future)


# pipeline stage -> worker: the inputs are wrapped to contain the job id in the wrapper
class BaseWorkerServerInputAdapter[
    InputT, OutputT,
    WorkerServerConfigT: BaseWorkerServerConfig
](BaseAsyncioBrokerInputAdapter[
    WorkerInputMessageWrapper[FrameJobInProgressMessage], FrameJobInProgressMessage
]):

    def __init__(self, *,
                 # the messages consumed from the broker are wrapped to contain the job id
                 # (still duplicated, possibly unneeded! -- perhaps refactor)
                 queue_to_consume: Queue[WorkerInputMessageWrapper[FrameJobInProgressMessage]],
                 worker_server: BaseWorkerServer[
                     FrameJobInProgressMessage, FrameJobInProgressMessage,
                     CriticalErrorMessage, InputT, OutputT, WorkerServerConfigT
                 ]):
        super().__init__(queue_to_consume)
        self._worker_server: BaseWorkerServer[
            FrameJobInProgressMessage, FrameJobInProgressMessage,
            CriticalErrorMessage, InputT, OutputT, WorkerServerConfigT
        ] = worker_server

    @override
    def _get_on_receive_func(self) -> Callable[[MessageWithAckFuture[FrameJobInProgressMessage]], None]:
        return self._worker_server.on_receive

    @override
    async def _on_startup(self) -> None:
        await self._worker_server.start()

    @override
    async def _on_shutdown(self) -> None:
        await self._worker_server.shutdown()

    @override
    def _convert_message_from_broker_to_worker(
            self, from_broker_msg: WorkerInputMessageWrapper[FrameJobInProgressMessage]
    ) -> FrameJobInProgressMessage:
        # unwrap
        return from_broker_msg.inputs_msg