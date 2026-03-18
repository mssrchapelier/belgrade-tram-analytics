import logging
from abc import ABC, abstractmethod
from asyncio import Event
from logging import Logger
from typing import override

from aio_pika.abc import AbstractRobustConnection, AbstractRobustChannel, AbstractRobustExchange, AbstractChannel
from pydantic import BaseModel


class BaseRabbitMQClientConfig(BaseModel, ABC):
    pass

class BaseRabbitMQClient[ConfigT: BaseRabbitMQClientConfig](ABC):

    def __init__(self, *, config: ConfigT, connection: AbstractRobustConnection) -> None:
        self._config: ConfigT = config
        self._logger: Logger = logging.

        self._connection: AbstractRobustConnection = connection

        self._is_running: bool = False
        self._is_stopping_event: Event = Event()

    @property
    def is_running(self) -> bool:
        return self._is_running

    @abstractmethod
    async def _on_start(self) -> None:
        pass

    async def start(self) -> None:
        if self._is_running:
            self._logger.warning("Requested startup but is already running; ignoring")
            return
        if self._is_stopping_event.is_set():
            self._logger.warning("Requested startup but is stopping; ignoring")
            return
        self._logger.debug("Starting")
        self._is_running = True
        await self._on_start()
        self._logger.debug("Started")

    async def stop(self) -> None:
        if not self._is_running:
            self._logger.warning("Requested a stop but is not running")
            return
        if self._is_stopping_event.is_set():
            self._logger.warning("Requested a stop but is already stopping; ignoring")
            return
        self._logger.debug("Stopping")
        self._is_stopping_event.set()
        await self._on_stop()
        self._is_running = False
        self._is_stopping_event.clear()
        self._logger.debug("Stopped")

    @abstractmethod
    async def _on_stop(self) -> None:
        pass

async def open_robust_channel(connection: AbstractRobustConnection) -> AbstractRobustChannel:
    # apparently a mypy bug; should be AbstractRobustChannel but does not recognise as such
    # it is important to ensure that the channel is robust, so a runtime check rather than simply casting
    channel: AbstractChannel = await connection.channel()
    assert isinstance(channel, AbstractRobustChannel)
    return channel

class BaseRabbitMQSingleChannelClient[ConfigT: BaseRabbitMQClientConfig](BaseRabbitMQClient[ConfigT], ABC):

    def __init__(self, *, config: ConfigT, connection: AbstractRobustConnection) -> None:
        super().__init__(config=config, connection=connection)
        self._channel: AbstractRobustChannel | None = None
        self._exchange: AbstractRobustExchange | None = None

    @override
    async def _on_start(self) -> None:
        await self._on_startup_before_opening_channel()
        await self._open_channel()
        await self._declare_exchange()
        await self._on_startup_after_exchange_declared()

    @abstractmethod
    async def _on_startup_before_opening_channel(self) -> None:
        pass

    @abstractmethod
    async def _on_startup_after_exchange_declared(self) -> None:
        pass

    @override
    async def _on_stop(self) -> None:
        await self._on_stop_before_closing_channel()
        await self._close_channel()
        await self._on_stop_after_closing_channel()

    @abstractmethod
    async def _on_stop_before_closing_channel(self) -> None:
        pass

    @abstractmethod
    async def _on_stop_after_closing_channel(self) -> None:
        pass

    async def _open_channel(self) -> None:
        if self._channel is not None:
            self._logger.warning("Requested opening a new channel but one is already opened")
            return
        self._channel = await open_robust_channel(self._connection)
        self._logger.debug("Opened channel")

    async def _close_channel(self) -> None:
        if self._channel is None:
            self._logger.warning("Requested closing the channel but none is opened")
            return
        await self._channel.close()
        self._channel = None
        self._logger.debug("Closed channel")

    @abstractmethod
    async def _declare_exchange(self) -> None:
        pass
