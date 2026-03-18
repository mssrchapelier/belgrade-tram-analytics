from typing import NamedTuple, override

# ----- IMPORTS -----

from tram_analytics.v2.pipeline._mock.common.dto.data_models import Square, SumSquares

# --- dependencies ---
# (1) MQ broker(s)
from src.v1_2.pipeline._mock.mq.asyncio_broker.pipeline_to_stages.pipeline_to_stages import PipelineToStagesAsyncioMQBroker
from src.v1_2.pipeline._mock.mq.asyncio_broker.stage_to_worker_server.stage_to_worker_server import StageToWorkerServerAsyncioMQBroker
# (2) persistence - inputs (retrieval)
from tram_analytics.v2.pipeline._mock.squarer.repository.dict_store.mock.mock_dict_store import SquarerMockDictStore
# (3) persistence - outputs (storage)
from archive.v2.src.pipeline._mock.summator.persistence import RunningSumSquaresPersistence, SummatorPersistenceConfig

# --- worker ---
from archive.v2.src.pipeline._mock.summator.summator import SummatorConfig, Summator

# --- worker server ---
# (1) worker
from archive.v2.src.pipeline._mock.summator.worker.worker import SummatorSyncWorker, SummatorSameThreadAsyncWorker
# (2) config
from tram_analytics.v2.pipeline._base.worker_servers.base_worker_server import BaseWorkerServerConfig
# (3) adapters
from archive.v2.src.pipeline._mock.summator.worker.adapters.mq.output_to_mq import SummatorWorkerMockMQBrokerOutputAdapter
from archive.v2.src.pipeline._mock.summator.worker.adapters.persistence_adapters import (
    SummatorReadRepositoryOutputPort, SummatorWriteRepositoryOutputPort
)
from src.v1_2.pipeline._mock.mq.asyncio_broker.stage_to_worker_server.input_adapter import (
    WorkerServerAsyncioBrokerInputAdapter
)
# (4) server
from archive.v2.src.pipeline._mock.summator.worker.worker_server import SummatorWorkerServer

# --- pipeline stage ---
# (1) config
from tram_analytics.v2.pipeline._base.pipeline_stage.base_pipeline_stage import BasePipelineStageConfig
# (2) adapters
from archive.v2.src.pipeline._mock.summator.pipeline_stage.adapters.mq.output.with_workers import (
    SummatorStageToWorkersMQBrokerOutputAdapter
)
from archive.v2.src.pipeline._mock.summator.pipeline_stage.adapters.mq.output.to_pipeline import (
    SummatorStageToPipelineMockMQBrokerOutputAdapter
)
# (3) stage
from tram_analytics.v2.pipeline._mock._base.service.pipeline_stage.base_pipeline_stage import PipelineStage

# --- wrapper for stage + worker server ---
from src.v1_2.pipeline._mock._base.integration.service.stage_with_own_worker_server.input_adapters.mq.asyncio_broker import (
    StageInputAdapter
)

# --- service ---
from tram_analytics.v2.pipeline._mock._base.service.integration.pipeline_step import BasePipelineStep

# ----- INTEGRATION -----

# worker server adapters
class SummatorWorkerServerOutputAdapters(NamedTuple):
    data_retrieval: SummatorReadRepositoryOutputPort
    data_persistence: SummatorWriteRepositoryOutputPort
    mq_to_pipeline: SummatorWorkerMockMQBrokerOutputAdapter

# pipeline stage adapters
class SummatorStageOutputAdapters(NamedTuple):
    to_workers: SummatorStageToWorkersMQBrokerOutputAdapter
    to_pipeline: SummatorStageToPipelineMockMQBrokerOutputAdapter

# SERVICE
class SummatorService(BasePipelineStep):

    def __init__(self, *,
                 # --- injected dependencies ---
                 broker: PipelineToStagesAsyncioMQBroker,
                 retrieval_persistence: SquarerMockDictStore,
                 # --- configs ---
                 persistence_config: SummatorPersistenceConfig,
                 worker_config: SummatorConfig,
                 worker_server_config: BaseWorkerServerConfig,
                 stage_config: BasePipelineStageConfig
                 ):
        self._pipeline_to_stage_broker: PipelineToStagesAsyncioMQBroker = broker
        self._stage_to_workers_broker: StageToWorkerServerAsyncioMQBroker = self._create_stage_to_workers_broker()

        self._retrieval_persistence: SquarerMockDictStore = retrieval_persistence

        self._persistence: RunningSumSquaresPersistence = self._create_persistence(persistence_config)
        self._worker: SummatorSameThreadAsyncWorker = self._create_worker(worker_config)

        self._worker_server_output_adapters: SummatorWorkerServerOutputAdapters = (
            self._create_output_adapters_for_worker_server()
        )
        self._worker_server: SummatorWorkerServer = self._create_worker_server(worker_server_config)
        self._worker_server_input_adapter: WorkerServerAsyncioBrokerInputAdapter[
            Square, SumSquares, BaseWorkerServerConfig
        ] = (
            self._create_input_adapter_for_worker_server()
        )

        self._stage_output_adapters: SummatorStageOutputAdapters = (
            self._create_output_adapters_for_stage()
        )
        self._stage: PipelineStage = self._create_stage(stage_config)
        # self._stage_input_adapter: SummatorStageMockPipelineStageAsyncioInputAdapter = (
        #     self._create_input_adapter_for_stage()
        # )

        self._wrapper_input_adapter: StageInputAdapter[
            Square, SumSquares, BaseWorkerServerConfig
        ] = self._create_wrapper_input_adapter()

    # --- BUILDING ---

    @staticmethod
    def _create_stage_to_workers_broker() -> StageToWorkerServerAsyncioMQBroker:
        return StageToWorkerServerAsyncioMQBroker()

    @staticmethod
    def _create_worker(config: SummatorConfig) -> SummatorSameThreadAsyncWorker:
        processor: Summator = Summator(config)
        sync_worker: SummatorSyncWorker = SummatorSyncWorker(processor)
        async_worker: SummatorSameThreadAsyncWorker = SummatorSameThreadAsyncWorker(sync_worker)
        return async_worker

    @staticmethod
    def _create_persistence(config: SummatorPersistenceConfig) -> RunningSumSquaresPersistence:
        return RunningSumSquaresPersistence(config)

    def _create_output_adapters_for_worker_server(self) -> SummatorWorkerServerOutputAdapters:
        return SummatorWorkerServerOutputAdapters(
            data_retrieval=SummatorReadRepositoryOutputPort(self._retrieval_persistence),
            data_persistence=SummatorWriteRepositoryOutputPort(self._persistence),
            mq_to_pipeline=SummatorWorkerMockMQBrokerOutputAdapter(self._stage_to_workers_broker)
        )

    def _create_worker_server(self, config: BaseWorkerServerConfig) -> SummatorWorkerServer:
        adapters: SummatorWorkerServerOutputAdapters = self._worker_server_output_adapters
        return SummatorWorkerServer(
            data_retrieval_adapter=adapters.data_retrieval,
            data_persistence_adapter=adapters.data_persistence,
            mq_to_pipeline_adapter=adapters.mq_to_pipeline,
            config=config,
            worker=self._worker
        )

    def _create_input_adapter_for_worker_server(self) -> WorkerServerAsyncioBrokerInputAdapter[
        Square, SumSquares, BaseWorkerServerConfig
    ]:
        return WorkerServerAsyncioBrokerInputAdapter(
            queue_to_consume=self._stage_to_workers_broker.stage_to_workers,
            worker_server=self._worker_server
        )

    def _create_output_adapters_for_stage(self) -> SummatorStageOutputAdapters:
        return SummatorStageOutputAdapters(
            to_workers=SummatorStageToWorkersMQBrokerOutputAdapter(self._stage_to_workers_broker),
            to_pipeline=SummatorStageToPipelineMockMQBrokerOutputAdapter(self._pipeline_to_stage_broker)
        )

    def _create_stage(self, config: BasePipelineStageConfig) -> PipelineStage:
        adapters: SummatorStageOutputAdapters = self._stage_output_adapters
        return PipelineStage(
            config=config,
            mq_to_workers_adapter=adapters.to_workers,
            mq_to_pipeline_adapter=adapters.to_pipeline
        )

    # def _create_input_adapter_for_stage(self) -> SummatorStageMockPipelineStageAsyncioInputAdapter:
    #     return SummatorStageMockPipelineStageAsyncioInputAdapter(
    #         broker=self._pipeline_to_stage_broker,
    #         stage=self._stage
    #     )

    def _create_wrapper_input_adapter(self) -> StageInputAdapter[
        Square, SumSquares, BaseWorkerServerConfig
    ]:
        return StageInputAdapter(
            main_broker_queue_to_consume=self._pipeline_to_stage_broker.in_progress.summator.to_stage,
            pipeline_stage=self._stage,
            worker_server_input_adapter=self._worker_server_input_adapter
        )

    # --- BEHAVIOUR ---

    @override
    async def start(self) -> None:
        await self._wrapper_input_adapter.start()

    @override
    async def shutdown(self) -> None:
        # await self._stage_input_adapter.shutdown()
        # await self._stage.shutdown()
        # await self._worker_server_input_adapter.shutdown()
        # await self._worker_server.shutdown()
        await self._wrapper_input_adapter.shutdown()
