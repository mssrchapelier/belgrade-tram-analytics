from abc import ABC
from typing import override

from aio_pika import Message

from tram_analytics.v2.mq.rabbit_mq.clients.base_clients.base_exchange_client import (
    BaseExchangeUserRabbitMQClientConfig, BaseExchangeUserRabbitMQClient
)


class ExchangeProducerRabbitMQClientConfig(BaseExchangeUserRabbitMQClientConfig):
    routing_key: str


class BaseExchangeProducerRabbitMQClient(
    BaseExchangeUserRabbitMQClient[ExchangeProducerRabbitMQClientConfig],
    ABC
):

    async def publish(self, msg: Message) -> None:
        if not self._is_running:
            self._logger.warning("Can't publish the message: not running")
        assert self._exchange is not None
        await self._exchange.publish(message=msg,
                                     routing_key=self._config.routing_key)
        self._logger.debug(f"Published message with routing key: {self._config.routing_key}")

    @override
    async def _on_startup_after_exchange_declared(self) -> None:
        pass

    @override
    async def _on_stop_before_closing_channel(self) -> None:
        pass


