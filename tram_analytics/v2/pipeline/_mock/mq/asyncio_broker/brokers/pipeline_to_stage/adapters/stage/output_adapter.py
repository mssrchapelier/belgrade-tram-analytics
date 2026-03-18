from abc import ABC, abstractmethod
from typing import override

from tram_analytics.v2.pipeline._mock._base.service.pipeline_stage.adapters.output_adapters.mq.to_pipeline import (
    BaseStageMQToPipelineOutputPort as Port
)
from tram_analytics.v2.pipeline._mock.common.dto.messages import (
    FrameJobInProgressMessage, DroppedJobMessage, CriticalErrorMessage
)
from tram_analytics.v2.pipeline._mock.mq.asyncio_broker.brokers.pipeline_to_stage.broker import \
    PipelineToStagesAsyncioMQBroker


class BaseStageMQToPipelineOutputAdapter(Port, ABC):

    def __init__(self, broker: PipelineToStagesAsyncioMQBroker) -> None:
        super().__init__()
        self._broker: PipelineToStagesAsyncioMQBroker = broker

    @override
    async def start(self) -> None:
        # initiate channels, etc.
        pass

    @override
    async def stop(self) -> None:
        pass

    @abstractmethod
    async def publish_for_completed_job(self, msg: FrameJobInProgressMessage) -> None:
        pass

    @abstractmethod
    async def publish_for_failed_job(self, msg: DroppedJobMessage) -> None:
        pass

    @abstractmethod
    async def report_critical_error(self, msg: CriticalErrorMessage) -> None:
        # send a message about a critical error before raising an exception
        pass