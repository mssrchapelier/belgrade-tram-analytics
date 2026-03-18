from abc import ABC
from typing import override

from tram_analytics.v2.pipeline._base.models.message import WorkerJobID
from tram_analytics.v2.pipeline._base.pipeline_stage.base_pipeline_stage import BasePipelineStage, \
    BasePipelineStageConfig
from tram_analytics.v2.pipeline._mock._base.service.pipeline_stage.adapters.output_adapters.mq.to_pipeline import (
    BaseStageMQToPipelineOutputPort
)
from tram_analytics.v2.pipeline._mock._base.service.pipeline_stage.adapters.output_adapters.mq.to_workers import (
    StageMQToWorkersOutputPort
)
from tram_analytics.v2.pipeline._mock.common.dto.messages import (
    FrameJobInProgressMessage, DroppedJobMessage, CriticalErrorMessage
)


class PipelineStageConfig(BasePipelineStageConfig, ABC):
    pass

class PipelineStage[ConfigT: PipelineStageConfig](
    BasePipelineStage[ConfigT, FrameJobInProgressMessage, DroppedJobMessage, CriticalErrorMessage],
    ABC
):

    def __init__(self,
                 *, config: ConfigT,
                 # pipeline stage to workers
                 mq_to_workers_adapter: StageMQToWorkersOutputPort,
                 # pipeline stage to pipeline
                 mq_to_pipeline_adapter: BaseStageMQToPipelineOutputPort
                 ):
        super().__init__(config=config,
                         mq_to_workers_adapter=mq_to_workers_adapter,
                         mq_to_pipeline_adapter=mq_to_pipeline_adapter)

    @override
    def _get_dropped_job_mq_message(
            self, job_id: WorkerJobID, exc: Exception
    ) -> DroppedJobMessage:
        return DroppedJobMessage(job_id=job_id,
                                 details=type(exc).__name__)

    @override
    def _get_critical_error_mq_message(
            self, job_id: WorkerJobID | None, exc: Exception
    ) -> CriticalErrorMessage:
        return CriticalErrorMessage(details=type(exc).__name__)