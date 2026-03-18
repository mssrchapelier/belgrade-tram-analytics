import logging
from logging import Logger
from types import TracebackType
from typing import Self, Type

import aio_pika
from aio_pika.abc import AbstractRobustConnection
from pydantic import BaseModel


class ConnectionParams(BaseModel):
    host: str
    port: int

class ConnectionManager:

    def __init__(self, config: ConnectionParams) -> None:
        self._connection: AbstractRobustConnection | None = None
        self._config: ConnectionParams = config
        self._logger: Logger = logging.

    @property
    def connection(self) -> AbstractRobustConnection:
        if self._connection is None:
            raise RuntimeError("The connection is not open")
        return self._connection

    async def __aenter__(self) -> Self:
        await self.open_connection()
        return self

    async def __aexit__(
            self, exc_type: Type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType
    ) -> None:
        await self.close_connection()

    async def open_connection(self) -> None:
        if self._connection is not None:
            self._logger.warning("Requested opening connection, but is already open")
            return
        self._connection = await aio_pika.connect_robust(
            host=self._config.host,
            port=self._config.port
        )
        self._logger.debug(f"Connected to broker: host {self._config.host}, "
                           f"port {self._config.port}")

    async def close_connection(self) -> None:
        if self._connection is None:
            self._logger.warning("Requested closing connection, but is closed")
            return
        await self._connection.close()
        self._logger.debug("Closed connection to broker")
        self._connection = None
