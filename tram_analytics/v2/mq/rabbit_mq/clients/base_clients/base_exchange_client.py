from abc import ABC
from typing import override

from aio_pika import ExchangeType
from pydantic import BaseModel

from tram_analytics.v2.mq.rabbit_mq.clients.base_clients.base_client import BaseRabbitMQClientConfig, \
    BaseRabbitMQSingleChannelClient


class ExchangeConfig(BaseModel):
    name: str
    exchange_type: ExchangeType
    durable: bool


class BaseExchangeUserRabbitMQClientConfig(BaseRabbitMQClientConfig, ABC):
    exchange: ExchangeConfig


class BaseExchangeUserRabbitMQClient[ConfigT: BaseExchangeUserRabbitMQClientConfig](
    BaseRabbitMQSingleChannelClient[ConfigT], ABC
):

    @override
    async def _declare_exchange(self) -> None:
        assert self._channel is not None
        if self._channel.is_closed:
            exc: RuntimeError = RuntimeError("Cannot declare the exchange: the channel is closed")
            self._logger.exception("Cannot declare the exchange", exc_info=exc)
            raise exc
        self._exchange = await self._channel.declare_exchange(
            name=self._config.exchange.name,
            type=self._config.exchange.exchange_type,
            durable=self._config.exchange.durable
        )
