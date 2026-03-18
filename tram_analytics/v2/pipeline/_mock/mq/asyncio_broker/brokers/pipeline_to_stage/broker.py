import asyncio
from asyncio import Queue, QueueShutDown, Task
from dataclasses import dataclass, field
from typing import Set, List, NamedTuple

from tram_analytics.v2.pipeline._mock.common.dto.messages import (
    FrameJobInProgressMessage, DroppedJobMessage, CriticalErrorMessage, IngestionDroppedItemMessage
)


# --- pipeline <-> stages in-process ---

@dataclass(frozen=True, slots=True, kw_only=True)
class QueuesForSingleStage:
    to_stage: Queue[FrameJobInProgressMessage] = field(default_factory=Queue)
    from_stage: Queue[FrameJobInProgressMessage] = field(default_factory=Queue)

@dataclass(frozen=True, slots=True, kw_only=True)
class QueuesForStages:
    squarer: QueuesForSingleStage = field(default_factory=QueuesForSingleStage)
    summator: QueuesForSingleStage = field(default_factory=QueuesForSingleStage)

class QueueTransferPair(NamedTuple):
    queue_from: Queue[FrameJobInProgressMessage]
    queue_to: Queue[FrameJobInProgressMessage]

class PipelineToStagesAsyncioMQBroker:
    """
    An in-process MQ broker stand-in to communicate between the pipeline and stages.
    """

    def __init__(self):
        # queues for stages
        self.in_progress: QueuesForStages = QueuesForStages()

        self.dropped_by_ingestion: Queue[IngestionDroppedItemMessage] = Queue()
        self.dropped_in_processing: Queue[DroppedJobMessage] = Queue()
        self.critical: Queue[CriticalErrorMessage] = Queue()

        self._queue_transfer_pairs: List[QueueTransferPair] = self._get_queue_transfer_pairs()
        self._transfer_tasks: Set[Task[None]] = self._start_transfer_loops()

        self._start_transfer_loops()

    def _get_queue_transfer_pairs(self) -> List[QueueTransferPair]:
        return [
            QueueTransferPair(queue_from=self.in_progress.squarer.from_stage,
                              queue_to=self.in_progress.summator.to_stage)
        ]

    def _start_transfer_loops(self) -> Set[Task[None]]:
        tasks: Set[Task[None]] = set()
        for pair in self._queue_transfer_pairs: # type: QueueTransferPair
            task: Task[None] = asyncio.create_task(
                self._loop_transfer_between_stages(queue_from=pair.queue_from,
                                                   queue_to=pair.queue_to)
            )
            tasks.add(task)
        return tasks

    async def shutdown(self) -> None:
        # shut down all queues from which items are being transferred
        for queue_pair in self._queue_transfer_pairs: # type: QueueTransferPair
            queue_pair.queue_from.shutdown()
        # wait for all transfer tasks to return
        await asyncio.wait(self._transfer_tasks,
                           return_when=asyncio.ALL_COMPLETED)

    async def _loop_transfer_between_stages(
            self, *, queue_from: Queue[FrameJobInProgressMessage], queue_to: Queue[FrameJobInProgressMessage]
    ) -> None:
        while True:
            try:
                item: FrameJobInProgressMessage = await queue_from.get()
                await queue_to.put(item)
            except QueueShutDown:
                break