from asyncio import Queue

from tram_analytics.v2.pipeline._base.models.message import WorkerInputMessageWrapper, WorkerOutputMessageWrapper
from tram_analytics.v2.pipeline._mock.common.dto.messages import FrameJobInProgressMessage, CriticalErrorMessage


# --- stage <-> workers in-process ---

class StageToWorkerServerAsyncioMQBroker:

    """
    An in-process MQ broker stand-in to communicate between the pipeline stage and the workers.
    """

    def __init__(self) -> None:
        self.stage_to_workers: Queue[WorkerInputMessageWrapper[FrameJobInProgressMessage]] = Queue()
        self.workers_to_stage: Queue[WorkerOutputMessageWrapper[FrameJobInProgressMessage]] = Queue()

        self.critical: Queue[CriticalErrorMessage] = Queue()
