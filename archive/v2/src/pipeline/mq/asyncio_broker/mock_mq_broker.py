from dataclasses import dataclass, field
from asyncio import Queue

from tram_analytics.v2.pipeline._base.models.message import WorkerOutputMessageWrapper, WorkerInputMessageWrapper
from tram_analytics.v2.pipeline._mock.common.dto.messages import FrameJobInProgressMessage

class MockMQBroker_SingleStage_Complete:

    def __init__(self) -> None:
        self.pipeline_to_stage: Queue[FrameJobInProgressMessage] = Queue()
        self.stage_to_workers: Queue[WorkerInputMessageWrapper[FrameJobInProgressMessage]] = Queue()
        self.workers_to_stage: Queue[WorkerOutputMessageWrapper[FrameJobInProgressMessage]] = Queue()
        self.stage_to_pipeline: Queue[FrameJobInProgressMessage] = Queue()

# --- stage <-> workers in-process ---

class MockStageToWorkersMQBroker:

    """
    An in-process MQ broker stand-in to communicate between the pipeline stage and the workers.
    """

    def __init__(self) -> None:
        self.stage_to_workers: Queue[WorkerInputMessageWrapper[FrameJobInProgressMessage]] = Queue()
        self.workers_to_stage: Queue[WorkerOutputMessageWrapper[FrameJobInProgressMessage]] = Queue()


# --- pipeline <-> stages in-process ---

@dataclass(frozen=True, slots=True, kw_only=True)
class QueuesForSingleStage:
    to_stage: Queue[FrameJobInProgressMessage] = field(default_factory=Queue)
    from_stage: Queue[FrameJobInProgressMessage] = field(default_factory=Queue)

@dataclass(frozen=True, slots=True, kw_only=True)
class QueuesForStages:
    squarer: QueuesForSingleStage = field(default_factory=QueuesForSingleStage)

class MockPipelineToStagesMQBroker:
    """
    An in-process MQ broker stand-in to communicate between the pipeline and stages.
    """

    def __init__(self):
        # queues for stages
        self.queues: QueuesForStages = QueuesForStages()
