import asyncio
from abc import ABC, abstractmethod
from asyncio import Event, Task
from typing import List, override, Never, Literal, Set, Any

from aio_pika.abc import AbstractRobustConnection, AbstractRobustQueue, AbstractIncomingMessage, AbstractQueueIterator
from pydantic import BaseModel, Field

from common.utils.concurrency.messaging.aio_pika_utils import get_next_from_aio_pika_queue_iterator
from tram_analytics.v2.mq.rabbit_mq.clients.base_clients.base_exchange_client import (
    BaseExchangeUserRabbitMQClientConfig, BaseExchangeUserRabbitMQClient
)


class ConsumerQueueConfig(BaseModel):
    name: str | None
    exclusive: bool
    durable: bool
    auto_delete: bool

class ExchangeConsumerRabbitMQClientConfig(BaseExchangeUserRabbitMQClientConfig):
    routing_keys: List[str] = Field(min_length=1)
    prefetch_count: int
    queue: ConsumerQueueConfig

class BaseExchangeConsumerRabbitMQClient(
    BaseExchangeUserRabbitMQClient[ExchangeConsumerRabbitMQClientConfig], ABC
):

    def __init__(self,
                 *, config: ExchangeConsumerRabbitMQClientConfig,
                 connection: AbstractRobustConnection) -> None:
        super().__init__(config=config, connection=connection)
        self._queue: AbstractRobustQueue | None = None
        self._num_cur_messages_in_handling: int = 0
        self._no_unhandled_messages_event: Event = Event()

        self._loop_queue_consuming_task: Task[None] | None = None

    @override
    async def _on_startup_after_exchange_declared(self) -> None:
        assert self._exchange is not None
        assert self._channel is not None
        # set prefetch limit
        await self._channel.set_qos(prefetch_count=self._config.prefetch_count)
        # declare the queue
        self._queue = await self._channel.declare_queue(name=self._config.queue.name,
                                                        exclusive=self._config.queue.exclusive,
                                                        durable=self._config.queue.durable,
                                                        auto_delete=self._config.queue.auto_delete)
        # bind to all routing keys
        for routing_key in self._config.routing_keys: # type: str
            await self._queue.bind(self._exchange,
                                   routing_key=routing_key)
        # start getting messages
        self._loop_queue_consuming_task = asyncio.create_task(
            self._loop_queue_consume()
        )

    @override
    async def _on_stop_before_closing_channel(self) -> None:
        # wait until the consumer stops
        if self._loop_queue_consuming_task is not None:
            await self._loop_queue_consuming_task
            self._loop_queue_consuming_task = None
        # wait until all unhandled tasks have been handled (messages acked etc.)
        await self._no_unhandled_messages_event.wait()

    def _log_and_raise_exc(self, msg: str) -> Never:
        self._logger.critical(msg)
        exc: RuntimeError = RuntimeError(msg)
        raise exc

    @abstractmethod
    async def _on_receive(self, msg: AbstractIncomingMessage) -> None:
        pass

    async def _handle_received_message(self, msg: AbstractIncomingMessage) -> None:
        self._num_cur_messages_in_handling += 1
        self._no_unhandled_messages_event.clear()

        await self._on_receive(msg)

        self._num_cur_messages_in_handling -= 1
        if self._num_cur_messages_in_handling == 0:
            self._no_unhandled_messages_event.set()

    class ReceivedStopSignalException(Exception):
        pass

    async def _get_while_listening_for_stop(self, queue_iter: AbstractQueueIterator) -> AbstractIncomingMessage:
        stop_signal_task: Task[Literal[True]] = asyncio.create_task(
            self._is_stopping_event.wait()
        )
        getting_task: Task[AbstractIncomingMessage] = asyncio.create_task(
            get_next_from_aio_pika_queue_iterator(queue_iter)
        )
        done, pending = await asyncio.wait(
            {getting_task, stop_signal_task},
            return_when=asyncio.FIRST_COMPLETED
        )  # type: Set[Task[Any]], Set[Task[Any]]
        for task in pending: # type: Task[Any]
            task.cancel()
        if stop_signal_task in done:
            raise self.ReceivedStopSignalException()
        assert getting_task in done
        item: AbstractIncomingMessage = await getting_task
        return item

    async def _loop_queue_consume(self) -> None:
        if not self._is_running:
            self._log_and_raise_exc("Tried to enter queue consuming loop but is not running; start first")
        if self._queue is None:
            self._log_and_raise_exc("Tried to enter queue consuming loop but the queue is not defined; start first")
        async with self._queue.iterator() as queue_iter: # type: AbstractQueueIterator
            while True:
                try:
                    msg: AbstractIncomingMessage = await self._get_while_listening_for_stop(queue_iter)
                    on_receive_task: Task[None] = asyncio.create_task(self._handle_received_message(msg))
                except self.ReceivedStopSignalException:
                    break
                except StopAsyncIteration as exc:
                    self._logger.exception("Consuming queue stopped iteration", exc_info=exc)
                    raise exc
