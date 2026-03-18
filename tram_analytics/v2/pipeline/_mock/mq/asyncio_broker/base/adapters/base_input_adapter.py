import asyncio
import logging
from abc import ABC, abstractmethod
from asyncio import Queue, Event, Task, Future
from logging import Logger
from typing import Literal, Set, Any, Callable

from common.utils.logging_utils.logging_utils import get_logger_name_for_object
from tram_analytics.v2.pipeline._base.models.message import BaseFrameJobInProgressMessage, MessageWithAckFuture, \
    WorkerJobID


class BaseAsyncioBrokerInputAdapter[
    FromBrokerInputJobMsgT, # either ToWorkerInputJobMsgT or WorkerInputMessageWrapper[ToWorkerInputJobMsgT], but not limiting to a union here
    ToConsumerInputJobMsgT: BaseFrameJobInProgressMessage
](ABC):

    """
    Entry point to either a pipeline stage or a worker server,
    consuming from an asyncio broker.
    Depends on: the queue to consume from the broker.

    Given a queue to consume and a function to call on each receive,
    after startup and before stopping, will get items from the queue,
    call the function with the message as the argument,
    and wait for an acknowledgement in a background task.
    A warning will be logged if a negative acknowledgement is received.
    """

    def __init__(
            self,
            # queue for messages with job requests
            queue_to_consume: Queue[FromBrokerInputJobMsgT]
    ) -> None:

        self._logger: Logger = logging.getLogger(get_logger_name_for_object(self))

        self._queue_to_consume: Queue[FromBrokerInputJobMsgT] = queue_to_consume

        self._is_running: bool = False
        self._is_shutting_down_event: Event = Event()
        self._is_shutting_down_event.clear()

        self._pull_from_mq_broker_task: Task[None] | None = None
        self._num_unacknowledged_messages: int = 0
        # shutdown will wait for all messages to be acked/nacked by the worker server
        self._no_unacknowledged_messages_event: Event = Event()
        self._no_unacknowledged_messages_event.set()

    @property
    def is_running(self) -> bool:
        # getter for the field for other classes
        return self._is_running

    @abstractmethod
    def _get_on_receive_func(self) -> Callable[[MessageWithAckFuture[ToConsumerInputJobMsgT]], None]:
        # return the function to call (from the wrapped consumer)
        # that accepts the input message (wrapped with ack future) for processing
        pass

    async def start(self) -> None:
        if self._is_running:
            self._logger.warning("This instance is already running")
            return
        if self._is_shutting_down_event.is_set():
            self._logger.warning("Requested startup but this instance is shutting down")
            return
        try:
            # start the worker server
            self._logger.debug("Starting")
            await self._on_startup()
            # start the message getting loop
            self._pull_from_mq_broker_task = asyncio.create_task(
                self._loop_pull_from_mq_broker()
            )
            self._is_running = True
            self._logger.debug("Started")
        except Exception as exc:
            self._logger.warning(f"Could not start, exception: {type(exc).__name__}")
            self._logger.debug("Exception when starting:\n", exc_info=exc)

    @abstractmethod
    async def _on_startup(self) -> None:
        # start broker server / pipeline stage
        pass

    async def shutdown(self) -> None:
        if not self._is_running:
            self._logger.warning("Requested shutdown but is not running")
            return
        if self._is_shutting_down_event.is_set():
            self._logger.warning("This instance is already shutting down")
            return
        try:
            self._logger.debug("Shutting down")
            self._is_running = False
            self._is_shutting_down_event.set()
            # waiting for messages to stop being pulled from the "broker"'s queue
            assert self._pull_from_mq_broker_task is not None
            await self._pull_from_mq_broker_task
            self._pull_from_mq_broker_task = None
            # waiting for the worker server to complete
            await self._no_unacknowledged_messages_event.wait()
            # stop the worker server
            await self._on_shutdown()
            self._logger.debug("Has shut down")
        except Exception as exc:
            self._logger.warning(f"Could not shut down, exception: {type(exc).__name__}")
            self._logger.debug("Exception when shutting down:\n", exc_info=exc)

    @abstractmethod
    async def _on_shutdown(self) -> None:
        # shut down broker server / pipeline stage
        pass

    async def _wait_for_acknowledgement(
            self, item_with_ack_future: MessageWithAckFuture[ToConsumerInputJobMsgT]
    ) -> None:
        """
        Wait for the acknowledgement of the input message from the wrapped processor,
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

    class ReceivedShutdownSignalException(Exception):
        pass

    async def _get_while_listening_for_shutdown(self) -> ToConsumerInputJobMsgT:
        shutdown_signal_task: Task[Literal[True]] = asyncio.create_task(
            self._is_shutting_down_event.wait()
        )
        getting_task: Task[FromBrokerInputJobMsgT] = asyncio.create_task(
            self._queue_to_consume.get()
        )
        done, pending = await asyncio.wait(
            {getting_task, shutdown_signal_task},
            return_when=asyncio.FIRST_COMPLETED
        )  # type: Set[Task[Any]], Set[Task[Any]]
        for task in pending: # type: Task[Any]
            task.cancel()
        if shutdown_signal_task in done:
            raise self.ReceivedShutdownSignalException()
        assert getting_task in done
        item: FromBrokerInputJobMsgT = await getting_task
        item_msg: ToConsumerInputJobMsgT = self._convert_message_from_broker_to_worker(item)
        return item_msg

    @abstractmethod
    def _convert_message_from_broker_to_worker(
            self, from_broker_msg: FromBrokerInputJobMsgT
    ) -> ToConsumerInputJobMsgT:
        # pass the same one if not wrapped; unwrap if wrapped
        pass

    async def _loop_pull_from_mq_broker(self) -> None:
        while True:
            try:
                # call here each time (in case the variable referring to the processor
                # of which the function is a method was reassigned)
                on_receive_func: Callable[[MessageWithAckFuture[ToConsumerInputJobMsgT]], None] = (
                    self._get_on_receive_func()
                )
                msg: ToConsumerInputJobMsgT = await self._get_while_listening_for_shutdown()
                self._logger.debug(f"Got message for job: {msg.get_job_id()}")
                item_with_ack_future: MessageWithAckFuture[ToConsumerInputJobMsgT] = MessageWithAckFuture(message=msg)
                # create a task to wait for acknowledgement of this message by the processor
                ack_task: Task[None] = asyncio.create_task(self._wait_for_acknowledgement(item_with_ack_future))
                self._num_unacknowledged_messages += 1
                self._no_unacknowledged_messages_event.clear()
                # trigger the processor (send the message to it)
                on_receive_func(item_with_ack_future)
            except self.ReceivedShutdownSignalException:
                break
