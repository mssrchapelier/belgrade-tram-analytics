from abc import ABC, abstractmethod
from typing import List, Dict, NamedTuple

from tram_analytics.v2.pipeline._base.worker_servers.base_worker_server import BaseWorkerServerConfig, \
    BaseWorkerServer
from tram_analytics.v2.pipeline._mock._base.service.pipeline_stage.base_pipeline_stage import PipelineStageConfig, \
    PipelineStage
from tram_analytics.v2.pipeline._mock.common.dto.messages import FrameJobInProgressMessage, CriticalErrorMessage


# --- for stateful steps ---

class StatefulPipelineStepInitData[PipelineStepConfigT](NamedTuple):
    camera_id: str
    config: PipelineStepConfigT

class BaseStatefulPipelineStepService[PipelineStepT, PipelineStepConfigT](ABC):

    """
    A service combining pipeline steps (one per camera)
    where each step consists of a dedicated stage and a (stateful) worker server.
    Examples: tracking, vehicle info, scene state.
    """

    def __init__(self, init_data_for_cameras: List[StatefulPipelineStepInitData[PipelineStepConfigT]]) -> None:
        self._cameras: Dict[str, PipelineStepT] = {
            init_data.camera_id: self._init_for_camera(init_data.config)
            for init_data in init_data_for_cameras
        }

    @abstractmethod
    def _init_for_camera(self, config: PipelineStepConfigT) -> PipelineStepT:
        pass

class StatelessStageInitData[
    ConfigT: PipelineStageConfig
](NamedTuple):
    camera_id: str
    config: ConfigT

# --- for stateless steps ---

# name -> worker server
class StatelessWorkerServerInitData[
    ConfigT: BaseWorkerServerConfig
](NamedTuple):
    worker_name: str
    config: ConfigT

# camera id -> stage
class StatelessPipelineStepInitData[
    StageConfigT: PipelineStageConfig,
    WorkerServerConfigT: BaseWorkerServerConfig
](NamedTuple):
    stages: List[StatelessStageInitData[StageConfigT]]
    worker_servers: List[StatelessWorkerServerInitData[WorkerServerConfigT]]

class BaseStatelessPipelineStepService[
    InputT, OutputT,
    StageConfigT: PipelineStageConfig,
    WorkerServerConfigT: BaseWorkerServerConfig
](ABC):

    """
    A service combining stages (one per camera) and stateless worker servers.
    To be used where the relationship between the two is not one-to-one.
    Examples: detection, feature extraction for re-ID, etc.
    """

    def __init__(self, init_data: StatelessPipelineStepInitData[StageConfigT, WorkerServerConfigT]) -> None:

        # camera id -> stage
        self._stages: Dict[str, PipelineStage[StageConfigT]] = {
            stage_data.camera_id: self._init_stage(stage_data.config)
            for stage_data in init_data.stages
        }

        # worker server name -> worker server
        self._worker_servers: Dict[str, BaseWorkerServer[
            FrameJobInProgressMessage, FrameJobInProgressMessage, CriticalErrorMessage,
            InputT, OutputT, WorkerServerConfigT
        ]] = {
            worker_data.worker_name: self._init_worker_server(worker_data.config)
            for worker_data in init_data.worker_servers
        }

    @abstractmethod
    def _init_stage(self, config: StageConfigT) -> PipelineStage[StageConfigT]:
        pass

    @abstractmethod
    def _init_worker_server(self, config: WorkerServerConfigT) -> BaseWorkerServer[
        FrameJobInProgressMessage, FrameJobInProgressMessage, CriticalErrorMessage,
        InputT, OutputT, WorkerServerConfigT
    ]:
        pass