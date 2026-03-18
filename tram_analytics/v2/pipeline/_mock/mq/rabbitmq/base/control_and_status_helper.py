import asyncio
from abc import ABC, abstractmethod
from asyncio import Event, Task
from typing import NamedTuple, override, Never, Literal, Set, Any, Tuple

from aio_pika import ExchangeType, Message, DeliveryMode
from aio_pika.abc import AbstractRobustChannel, AbstractRobustExchange, AbstractRobustQueue, AbstractRobustConnection, \
    AbstractIncomingMessage, AbstractQueueIterator
from pydantic import BaseModel

from common.utils.concurrency.messaging.aio_pika_utils import get_next_from_aio_pika_queue_iterator
from tram_analytics.v2.mq.rabbit_mq.clients.base_clients.base_client import BaseRabbitMQClientConfig, \
    BaseRabbitMQClient, \
    open_robust_channel
from tram_analytics.v2.pipeline._mock.common.dto.messages import ControlCommand, ControlMessage, ComponentStatus, \
    StatusMessage

# --- configs ---

CONTROL_MSG_EXCHANGE_NAME: str = "control_messages"
STATUS_MSG_EXCHANGE_NAME: str = "status_messages"

class ControlBrokerContext(NamedTuple):
    channel: AbstractRobustChannel
    exchange: AbstractRobustExchange
    queue: AbstractRobustQueue

class StatusBrokerContext(NamedTuple):
    channel: AbstractRobustChannel
    exchange: AbstractRobustExchange

class ControlContextConfig(BaseModel):
    routing_key: str

class StatusContextConfig(BaseModel):
    routing_key: str
    # How often to send "is running" status messages whilst running.
    running_status_producing_frequency: float

class RabbitMQControlAndStatusReportWrapperConfig(BaseRabbitMQClientConfig):
    # control msg channel, for consuming
    control: ControlContextConfig
    # status msg channel, for producing
    status: StatusContextConfig

# --- client ---

class BaseControlAndStatusInputAdapter[WrappedClientT](
    BaseRabbitMQClient[RabbitMQControlAndStatusReportWrapperConfig], ABC
):
    """
    Wrapper for a component that:
    (1) consumes control messages for the wrapped component (start/shutdown);
    (2) emits status messages (running/stopped) for that component.

    BEHAVIOUR:
    - `start`, `stop` control starting/stopping consuming control messages and emitting status messages.
    - On receiving a "start" control message, starts the wrapped client and publishes a "running" status message.
    - On receiving a "stop" control message, stop the wrapped client and publishes a "stopped" status message.
    - While the wrapped client is running, publishes a "stopped" status message
      every `running_status_producing_frequency` seconds.

    MOTIVATION:
    To allow controlling arbitrary startable/stoppable objects by control messages,
    and to publish their "running" status while they are running
    (and a "stopped" message once they have been stopped).
    """
    # TODO: possibly make WrappedClientT subtype a protocol

    def __init__(self, *,
                 wrapped_client: WrappedClientT,
                 connection: AbstractRobustConnection,
                 config: RabbitMQControlAndStatusReportWrapperConfig
                 ):
        super().__init__(config=config, connection=connection)

        self._wrapped_client: WrappedClientT = wrapped_client

        self._is_wrapped_client_running_event: Event = Event()
        self._is_wrapped_client_stopped_event: Event = Event()

        if self._get_is_running_wrapped_client(self._wrapped_client):
            self._is_wrapped_client_running_event.set()
        else:
            self._is_wrapped_client_stopped_event.set()

        self._control_context: ControlBrokerContext | None = None
        self._status_context: StatusBrokerContext | None = None

        self._control_msg_consuming_task: Task[None] | None = None
        self._status_msg_publishing_task: Task[None] | None = None

    @abstractmethod
    def _get_is_running_wrapped_client(self, wrapped_client: WrappedClientT) -> bool:
        pass

    async def _start_wrapped_client_and_set_flag(self, wrapped_client: WrappedClientT) -> None:
        await self._start_wrapped_client(wrapped_client)
        self._is_wrapped_client_stopped_event.clear()
        self._is_wrapped_client_running_event.set()

    @abstractmethod
    async def _start_wrapped_client(self, wrapped_client: WrappedClientT) -> None:
        pass

    async def _stop_wrapped_client_and_set_flag(self, wrapped_client: WrappedClientT) -> None:
        await self._stop_wrapped_client(wrapped_client)
        self._is_wrapped_client_running_event.clear()
        self._is_wrapped_client_stopped_event.set()

    @abstractmethod
    async def _stop_wrapped_client(self, wrapped_client: WrappedClientT) -> None:
        pass

    @override
    async def _on_start(self) -> None:
        await self._init_control_context()
        await self._init_status_context()
        if self._control_msg_consuming_task is None:
            self._control_msg_consuming_task = asyncio.create_task(
                self._loop_control_messages_consume()
            )
        if self._status_msg_publishing_task is None:
            self._status_msg_publishing_task = asyncio.create_task(
                self._loop_status_messages_publish()
            )

    @override
    async def _on_stop(self) -> None:
        if self._status_msg_publishing_task is not None:
            await self._status_msg_publishing_task
        if self._control_msg_consuming_task is not None:
            await self._control_msg_consuming_task
        await self._destroy_status_context()
        await self._destroy_control_context()

    async def _init_control_context(self) -> None:
        assert self._control_context is None
        channel: AbstractRobustChannel = await open_robust_channel(self._connection)
        exchange: AbstractRobustExchange = await channel.declare_exchange(
            name=CONTROL_MSG_EXCHANGE_NAME,
            type=ExchangeType.TOPIC,
            durable=True
        )
        routing_key: str = self._config.control.routing_key
        queue: AbstractRobustQueue = await channel.declare_queue(
            # name the same as the routing key
            name=routing_key,
            exclusive=False, durable=True, auto_delete=False
        )
        await queue.bind(exchange, routing_key=routing_key)
        self._control_context = ControlBrokerContext(channel, exchange, queue)
        self._logger.debug("Initialised control context")

    async def _init_status_context(self) -> None:
        assert self._status_context is None
        channel: AbstractRobustChannel = await open_robust_channel(self._connection)
        exchange: AbstractRobustExchange = await channel.declare_exchange(
            name=STATUS_MSG_EXCHANGE_NAME,
            type=ExchangeType.TOPIC,
            durable=True
        )
        self._status_context = StatusBrokerContext(channel, exchange)
        self._logger.debug("Initialised status context")

    async def _destroy_control_context(self) -> None:
        assert self._control_context is not None
        await self._control_context.channel.close()
        self._control_context = None
        self._logger.debug("Destroyed control context")

    async def _destroy_status_context(self) -> None:
        assert self._status_context is not None
        await self._status_context.channel.close()
        self._status_context = None
        self._logger.debug("Destroyed status context")

    def _log_and_raise_exc(self, msg: str) -> Never:
        self._logger.critical(msg)
        exc: RuntimeError = RuntimeError(msg)
        raise exc

    # (1) loop: listen for control messages and dispatch start / shutdown

    class ReceivedOwnStopSignalException(Exception):
        """
        To be raised when THIS instance is stopping
        (NOTE: NOT when receiving a control message for the WRAPPED client to stop).
        """
        pass

    async def _take_control_action_on_wrapped_client(self, msg_dto: ControlMessage) -> None:
        """
        Parse the command from the control message and command the wrapped client to start/stop.
        """
        match msg_dto.command:
            case ControlCommand.START:
                if not self._is_wrapped_client_running_event.is_set():
                    await self._start_wrapped_client_and_set_flag(self._wrapped_client)
                    self._is_wrapped_client_stopped_event.clear()
                    self._is_wrapped_client_running_event.set()
            case ControlCommand.STOP:
                if not self._is_wrapped_client_stopped_event.is_set():
                    await self._stop_wrapped_client_and_set_flag(self._wrapped_client)
                    self._is_wrapped_client_running_event.clear()
                    self._is_wrapped_client_stopped_event.set()
            case _:
                raise ValueError(f"Unknown command: {msg_dto.command}")

    async def _on_receive_control_msg(self, msg: AbstractIncomingMessage) -> None:
        """
        Handle a control message received for the wrapped object.
        """
        async with msg.process(ignore_processed=True):
            # call start / shutdown on the wrapped object
            try:
                msg_dto: ControlMessage = ControlMessage.model_validate_json(msg.body)
                await self._take_control_action_on_wrapped_client(msg_dto)
                await msg.ack()
            except Exception as exc:
                # NOT fatal -- do not raise, just log
                exc_descr: str = "Exception when processing control message"
                self._logger.critical(exc_descr)
                self._logger.debug(exc_descr, exc_info=exc)
                # reject but do NOT requeue.
                # This is a control message; simple re-delivery to this same consumer will make no sense.
                # Rather, the orchestrator is monitoring status messages,
                # and it is its responsibility to re-issue the command if it judges it necessary.
                await msg.nack(requeue=False)

    async def _get_control_msg_while_listening_for_own_stopping(
            self, queue_iter: AbstractQueueIterator
    ) -> AbstractIncomingMessage:
        # until this instance has received a stopping signal
        own_stopping_signal_task: Task[Literal[True]] = asyncio.create_task(
            self._is_stopping_event.wait()
        )
        # or until got a control message
        control_message_getting_task: Task[AbstractIncomingMessage] = asyncio.create_task(
            get_next_from_aio_pika_queue_iterator(queue_iter)
        )
        done, pending = await asyncio.wait(
            {control_message_getting_task, own_stopping_signal_task},
            return_when=asyncio.FIRST_COMPLETED
        )  # type: Set[Task[Any]], Set[Task[Any]]
        for task in pending: # type: Task[Any]
            task.cancel()
        if own_stopping_signal_task in done:
            raise self.ReceivedOwnStopSignalException()
        assert control_message_getting_task in done
        item: AbstractIncomingMessage = await control_message_getting_task
        return item

    async def _loop_control_messages_consume(self) -> None:
        if not self._is_running:
            self._log_and_raise_exc("Tried to enter control queue consuming loop "
                                    "but is not running; start first")
        if self._control_context is None:
            self._log_and_raise_exc("Tried to enter control queue consuming loop "
                                    "but the control context is not defined; start first")
        async with self._control_context.queue.iterator() as queue_iter: # type: AbstractQueueIterator
            while True:
                try:
                    msg: AbstractIncomingMessage = await (
                        self._get_control_msg_while_listening_for_own_stopping(queue_iter)
                    )
                    on_receive_task: Task[None] = asyncio.create_task(
                        self._on_receive_control_msg(msg)
                    )
                except self.ReceivedOwnStopSignalException:
                    break
                except StopAsyncIteration as exc:
                    self._logger.exception("Consuming queue stopped iteration", exc_info=exc)
                    raise exc

    # (2) loop: while not stopped, send running messages periodically
    # also send messages when starting running; when stopped (once)

    async def _wait_for_client_to_run_or_for_own_stopping(self) -> Tuple[bool, bool]:
        # wait for either of the two events (whichever occurs earlier):
        # (a) the wrapped client is running
        # (b) this instance is stopped
        own_stopping_signal_task: Task[Literal[True]] = asyncio.create_task(
            self._is_stopping_event.wait()
        )
        wrapped_client_running_signal_task: Task[Literal[True]] = asyncio.create_task(
            self._is_wrapped_client_running_event.wait()
        )
        done, pending = await asyncio.wait(
            {own_stopping_signal_task, wrapped_client_running_signal_task},
            return_when=asyncio.FIRST_COMPLETED
        )  # type: Set[Task[Any]], Set[Task[Any]]
        for task in pending:  # type: Task[Any]
            task.cancel()
        return (own_stopping_signal_task in done,
                wrapped_client_running_signal_task in done)

    async def _wait_for_timer_or_client_stop(self) -> Tuple[bool, bool]:
        # wait for either of the two events (whichever occurs earlier):
        # (a) the wrapped client is stopping
        # (b) the timer to publish the next "is running" status message has expired
        timer_to_publish_status_task: Task[None] = asyncio.create_task(
            asyncio.sleep(self._config.status.running_status_producing_frequency)
        )
        client_stopped_task: Task[Literal[True]] = asyncio.create_task(
            self._is_wrapped_client_stopped_event.wait()
        )
        done, pending = await asyncio.wait(
            {timer_to_publish_status_task, client_stopped_task},
            return_when=asyncio.FIRST_COMPLETED
        )  # type: Set[Task[Any]], Set[Task[Any]]
        for task in pending:  # type: Task[Any]
            task.cancel()
        return (timer_to_publish_status_task in done,
                client_stopped_task in done)

    async def _publish_status_message(self, status: ComponentStatus) -> None:
        if self._status_context is None:
            self._log_and_raise_exc("Tried to publish a status message, "
                                    "but this instance's status context is not defined")
        message_dto: StatusMessage = StatusMessage(status=status)
        as_str: str = message_dto.model_dump_json()
        serialised: bytes = as_str.encode(encoding="utf8")
        msg: Message = Message(body=serialised,
                               delivery_mode=DeliveryMode.PERSISTENT,
                               timestamp=message_dto.timestamp)
        await self._status_context.exchange.publish(
            msg,
            routing_key=self._config.status.routing_key
        )

    async def _loop_status_messages_publish(self) -> None:
        if not self._is_running:
            self._log_and_raise_exc("Tried to enter status queue publishing loop "
                                    "but is not running; start first")
        if self._status_context is None:
            self._log_and_raise_exc("Tried to enter status queue publishing loop "
                                    "but the status context is not defined; start first")
        while self._is_running:
            if not self._is_wrapped_client_running_event.is_set():
                is_self_stopping, is_wrapped_client_running = await (
                    self._wait_for_client_to_run_or_for_own_stopping()
                ) # type: bool, bool
                if is_self_stopping:
                    break
                assert is_wrapped_client_running
                await self._publish_status_message(ComponentStatus.RUNNING)
            # wrapped client running
            # - wait until timeout to publish RUNNING or until is stopped
            is_timer_to_publish_running_status_expired, is_wrapped_client_stopped = (
                await self._wait_for_timer_or_client_stop()
            )
            if is_wrapped_client_stopped:
                # will have published a stopped status during stop; do not publish anything
                await self._publish_status_message(ComponentStatus.STOPPED)
            else:
                assert is_timer_to_publish_running_status_expired
                # publish the periodic "is running" status message
                await self._publish_status_message(ComponentStatus.RUNNING)
