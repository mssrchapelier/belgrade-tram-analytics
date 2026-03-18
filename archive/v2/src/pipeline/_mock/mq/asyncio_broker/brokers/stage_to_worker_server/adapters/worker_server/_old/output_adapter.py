from typing import override
from abc import ABC
from asyncio import Queue

from tram_analytics.v2.pipeline._base.worker_servers.adapters.output_adapters.mq.to_stage import BaseWorkerServerMQOutputPort
from tram_analytics.v2.pipeline._base.models.message import WorkerOutputMessageWrapper
from tram_analytics.v2.pipeline._mock.common.dto.messages import FrameJobInProgressMessage, CriticalErrorMessage

# between the pipeline stage and workers // for the worker, output

class BaseWorkerServerMQOutputAdapter(
    BaseWorkerServerMQOutputPort[FrameJobInProgressMessage, CriticalErrorMessage],
    ABC
):
    """
    An output adapter for the worker server that produces messages
    from an asyncio queue-based broker (in-process, in-thread).
    Used for communication between the worker server
    and either the stage or the main orchestration component
    where the broker is lightweight and runs in the same process,
    to avoid the need for an external broker in this case.
    """

    def __init__(self,
                 *, queue_for_publishing: Queue[WorkerOutputMessageWrapper[FrameJobInProgressMessage]],
                 queue_for_critical: Queue[CriticalErrorMessage]) -> None:
        super().__init__()
        self._queue_for_publishing: Queue[WorkerOutputMessageWrapper[FrameJobInProgressMessage]] = queue_for_publishing
        self._queue_for_critical: Queue[CriticalErrorMessage] = queue_for_critical

    @override
    async def publish_for_completed_job(self, msg: WorkerOutputMessageWrapper[FrameJobInProgressMessage]) -> None:
        await self._queue_for_publishing.put(msg)

    @override
    async def report_critical_error(self, msg: CriticalErrorMessage) -> None:
        await self._queue_for_critical.put(msg)
