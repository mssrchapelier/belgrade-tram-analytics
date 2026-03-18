from typing import override

from tram_analytics.v2.pipeline._mock.common.dto.messages import (
    FrameJobInProgressMessage, DroppedJobMessage, CriticalErrorMessage
)
from tram_analytics.v2.pipeline._mock.mq.asyncio_broker.brokers.pipeline_to_stage.adapters.stage.output_adapter import (
    BaseStageMQToPipelineOutputAdapter
)


class SummatorStageAsyncioBrokerMQToPipelineOutputAdapter(BaseStageMQToPipelineOutputAdapter):

    @override
    async def publish_for_completed_job(self, msg: FrameJobInProgressMessage) -> None:
        await self._broker.in_progress.summator.from_stage.put(msg)

    @override
    async def publish_for_failed_job(self, msg: DroppedJobMessage) -> None:
        await self._broker.dropped_in_processing.put(msg)

    @override
    async def report_critical_error(self, msg: CriticalErrorMessage) -> None:
        # send a message about a critical error before raising an exception
        await self._broker.critical.put(msg)