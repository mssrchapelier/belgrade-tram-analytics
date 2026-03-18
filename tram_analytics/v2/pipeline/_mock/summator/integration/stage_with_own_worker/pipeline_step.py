from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from typing import NamedTuple, override, TypeAlias

from tram_analytics.v2.pipeline._base.worker_servers.workers.async_workers.base_async_workers import \
    BaseOrderedAsyncWorker
# --- base stream pipeline step ---
from tram_analytics.v2.pipeline._mock._base.service.integration.pipeline_step import BasePipelineStep
# --- wrapper for stage + worker server ---
# - adapters
# --- input
from tram_analytics.v2.pipeline._mock._base.service.integration.stage_worker_server_bundled.adapters.input_adapters.asyncio_broker import (
    BaseStageAsyncioBrokerInputAdapter
)
# (for type hints)
from tram_analytics.v2.pipeline._mock.common.dto.data_models import Square, SumSquares
# --- dependencies ---
# (1) MQ broker(s)
from tram_analytics.v2.pipeline._mock.mq.asyncio_broker.brokers.pipeline_to_stage.broker import \
    PipelineToStagesAsyncioMQBroker
# - output
from tram_analytics.v2.pipeline._mock.mq.asyncio_broker.brokers.stage_to_worker_server.adapters.worker_server.output_adapter import \
    WorkerServerAsyncioBrokerOutputAdapter
from tram_analytics.v2.pipeline._mock.mq.asyncio_broker.brokers.stage_to_worker_server.broker import \
    StageToWorkerServerAsyncioMQBroker
# (2) data - inputs (retrieval)
from tram_analytics.v2.pipeline._mock.squarer.repository.dict_store.mock.mock_dict_store import SquarerMockDictStore
# --- pipeline stage ---
# (1) adapters
# - input
# ... none for stage -- integrated for stage + worker server -- see below ...
# - output
from tram_analytics.v2.pipeline._mock.summator.pipeline_stage.adapters.output_adapters.mq.to_pipeline.asyncio_broker import (
    SummatorStageAsyncioBrokerMQToPipelineOutputAdapter
)
# ... MQ to workers ...
from tram_analytics.v2.pipeline._mock.summator.pipeline_stage.adapters.output_adapters.mq.to_workers.asyncio_broker import (
    SummatorStageAsyncioBrokerMQToWorkersAdapter
)
# (2) stage
from tram_analytics.v2.pipeline._mock.summator.pipeline_stage.stage import SummatorPipelineStageConfig, \
    SummatorPipelineStage
# (3) data - outputs (storage)
from tram_analytics.v2.pipeline._mock.summator.repository.dict_store.mock.mock_dict_store import \
    SummatorMockDictStore, SummatorMockDictStoreConfig
# (3) adapters
# - input
from tram_analytics.v2.pipeline._mock.summator.worker_server.adapters.input_adapters.asyncio_broker import \
    WorkerServerInputAdapter
from tram_analytics.v2.pipeline._mock.summator.worker_server.adapters.output_adapters.repository.read_repo.mock_dict_store import \
    SummatorReadMockDictStore
from tram_analytics.v2.pipeline._mock.summator.worker_server.adapters.output_adapters.repository.write_repo.mock_dict_store import \
    SummatorWriteMockDictStore
from tram_analytics.v2.pipeline._mock.summator.worker_server.worker.async_worker.same_thread import \
    SummatorAsyncSameThreadWorker
# --- worker server ---
# (1) worker
from tram_analytics.v2.pipeline._mock.summator.worker_server.worker.async_worker.separate_thread import \
    SummatorAsyncThreadExecutorWorker
# --- processor ---
from tram_analytics.v2.pipeline._mock.summator.worker_server.worker.core.summator import SummatorConfig, Summator
from tram_analytics.v2.pipeline._mock.summator.worker_server.worker.sync_worker import SummatorSyncWorker
# (4) server
from tram_analytics.v2.pipeline._mock.summator.worker_server.worker_server import SummatorWorkerServer
# (2) config
from tram_analytics.v2.pipeline._mock.summator.worker_server.worker_server import SummatorWorkerServerConfig

# ----- IMPORTS -----

StageAsyncioBrokerInputAdapter: TypeAlias = BaseStageAsyncioBrokerInputAdapter[
    Square, SumSquares, SummatorPipelineStageConfig, SummatorWorkerServerConfig
]

# ----- INTEGRATION -----

# worker server adapters
class WorkerServerOutputAdapters(NamedTuple):
    data_retrieval: SummatorReadMockDictStore
    data_persistence: SummatorWriteMockDictStore
    mq_to_pipeline: WorkerServerAsyncioBrokerOutputAdapter

# pipeline stage adapters
class StageOutputAdapters(NamedTuple):
    to_workers: SummatorStageAsyncioBrokerMQToWorkersAdapter
    to_pipeline: SummatorStageAsyncioBrokerMQToPipelineOutputAdapter

# PIPELINE STEP for a single stream
class BaseSummatorPipelineStep(BasePipelineStep, ABC):

    def __init__(self, *,
                 # broker - pipeline to stage
                 broker: PipelineToStagesAsyncioMQBroker,
                 # source data store
                 src_data_repo: SquarerMockDictStore,
                 # dest data store
                 dest_data_repo_config: SummatorMockDictStoreConfig,
                 # worker
                 # --- configs ---
                 worker_config: SummatorConfig,
                 worker_server_config: SummatorWorkerServerConfig,
                 stage_config: SummatorPipelineStageConfig
                 ):
        self._pipeline_to_stage_broker: PipelineToStagesAsyncioMQBroker = broker
        self._stage_to_workers_broker: StageToWorkerServerAsyncioMQBroker = self._create_stage_to_workers_broker()

        self._src_data_repo: SquarerMockDictStore = src_data_repo
        self._dest_data_repo: SummatorMockDictStore = self._create_dest_data_repo(dest_data_repo_config)

        self._core_processor: Summator = Summator(worker_config)
        self._sync_worker: SummatorSyncWorker = SummatorSyncWorker(self._core_processor)
        self._async_worker: BaseOrderedAsyncWorker[Square, SumSquares] = self._create_async_worker()

        self._worker_server_output_adapters: WorkerServerOutputAdapters = (
            self._create_worker_server_output_adapters()
        )
        self._worker_server: SummatorWorkerServer = self._create_worker_server(worker_server_config)
        self._worker_server_input_adapter: WorkerServerInputAdapter = (
            self._create_worker_server_input_adapter()
        )

        self._stage_output_adapters: StageOutputAdapters = (
            self._create_stage_output_adapters()
        )
        self._stage: SummatorPipelineStage = self._create_stage(stage_config)

        self._input_adapter: StageAsyncioBrokerInputAdapter = self._create_wrapper_input_adapter()


    # --- BUILDING ---

    @staticmethod
    def _create_stage_to_workers_broker() -> StageToWorkerServerAsyncioMQBroker:
        return StageToWorkerServerAsyncioMQBroker()

    @staticmethod
    def _create_dest_data_repo(config: SummatorMockDictStoreConfig) -> SummatorMockDictStore:
        return SummatorMockDictStore(config)

    @abstractmethod
    def _create_async_worker(self) -> BaseOrderedAsyncWorker[Square, SumSquares]:
        pass

    def _create_worker_server_output_adapters(self) -> WorkerServerOutputAdapters:
        return WorkerServerOutputAdapters(
            data_retrieval=SummatorReadMockDictStore(self._src_data_repo),
            data_persistence=SummatorWriteMockDictStore(self._dest_data_repo),
            mq_to_pipeline=WorkerServerAsyncioBrokerOutputAdapter(self._stage_to_workers_broker)
        )

    def _create_worker_server(self, config: SummatorWorkerServerConfig) -> SummatorWorkerServer:
        adapters: WorkerServerOutputAdapters = self._worker_server_output_adapters
        return SummatorWorkerServer(
            read_repo=adapters.data_retrieval,
            write_repo=adapters.data_persistence,
            mq_to_pipeline=adapters.mq_to_pipeline,
            config=config,
            worker=self._async_worker
        )

    def _create_worker_server_input_adapter(self) -> WorkerServerInputAdapter:
        return WorkerServerInputAdapter(
            queue_to_consume=self._stage_to_workers_broker.stage_to_workers,
            worker_server=self._worker_server
        )

    def _create_stage_output_adapters(self) -> StageOutputAdapters:
        return StageOutputAdapters(
            to_workers=SummatorStageAsyncioBrokerMQToWorkersAdapter(self._stage_to_workers_broker),
            to_pipeline=SummatorStageAsyncioBrokerMQToPipelineOutputAdapter(self._pipeline_to_stage_broker)
        )

    def _create_stage(self, config: SummatorPipelineStageConfig) -> SummatorPipelineStage:
        adapters: StageOutputAdapters = self._stage_output_adapters
        return SummatorPipelineStage(
            config=config,
            mq_to_workers_adapter=adapters.to_workers,
            mq_to_pipeline_adapter=adapters.to_pipeline
        )

    def _create_wrapper_input_adapter(self) -> StageAsyncioBrokerInputAdapter:
        return StageAsyncioBrokerInputAdapter(
            main_broker_queue_to_consume=self._pipeline_to_stage_broker.in_progress.summator.to_stage,
            pipeline_stage=self._stage,
            worker_server_input_adapter=self._worker_server_input_adapter
        )

    # --- BEHAVIOUR ---

    @override
    async def start(self) -> None:
        await self._input_adapter.start()

    @override
    async def shutdown(self) -> None:
        await self._input_adapter.shutdown()
        
class SummatorSameThreadPipelineStep(BaseSummatorPipelineStep):

    @override
    def _create_async_worker(self) -> SummatorAsyncSameThreadWorker:
        return SummatorAsyncSameThreadWorker(sync_worker=self._sync_worker)

class SummatorThreadExecutorWorkerPipelineStep(BaseSummatorPipelineStep):

    def __init__(self, executor: ThreadPoolExecutor,
                 **kwargs):
        self._executor: ThreadPoolExecutor = executor
        super().__init__(**kwargs)

    @override
    def _create_async_worker(self) -> SummatorAsyncThreadExecutorWorker:
        return SummatorAsyncThreadExecutorWorker(sync_worker=self._sync_worker,
                                                 executor=self._executor)