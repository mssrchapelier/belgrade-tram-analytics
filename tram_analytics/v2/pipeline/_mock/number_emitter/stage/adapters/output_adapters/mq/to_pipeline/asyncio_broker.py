from typing import override

from tram_analytics.v2.pipeline._base.ingestion_stage.adapters.output_adapters.mq.to_pipeline import \
    BaseIngestionMQToPipeline
from tram_analytics.v2.pipeline._mock.common.dto.messages import (
    FrameJobInProgressMessage, CriticalErrorMessage, IngestionDroppedItemMessage
)
from tram_analytics.v2.pipeline._mock.mq.asyncio_broker.brokers.pipeline_to_stage.broker import \
    PipelineToStagesAsyncioMQBroker


class StageAsyncioBrokerMQOutputAdapter(
    BaseIngestionMQToPipeline[
        FrameJobInProgressMessage, IngestionDroppedItemMessage, CriticalErrorMessage
    ]
):

    def __init__(self, broker: PipelineToStagesAsyncioMQBroker) -> None:
        super().__init__()
        self._broker: PipelineToStagesAsyncioMQBroker = broker

    @override
    async def start(self) -> None:
        pass

    @override
    async def stop(self) -> None:
        pass

    @override
    async def publish_for_emitted_item(self, msg: FrameJobInProgressMessage) -> None:
        await self._broker.in_progress.squarer.to_stage.put(msg)

    @override
    async def publish_for_dropped_item(self, msg: IngestionDroppedItemMessage) -> None:
        await self._broker.dropped_by_ingestion.put(msg)

    @override
    async def report_critical_error(self, msg: CriticalErrorMessage) -> None:
        # send a message about a critical error before raising an exception
        await self._broker.critical.put(msg)