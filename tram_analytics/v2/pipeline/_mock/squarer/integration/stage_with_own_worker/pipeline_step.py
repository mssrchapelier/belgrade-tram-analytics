from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from typing import NamedTuple, override, TypeAlias

from tram_analytics.v2.pipeline._base.worker_servers.workers.async_workers.base_async_workers import \
    BaseUnorderedAsyncWorker
# --- base stream pipeline step ---
from tram_analytics.v2.pipeline._mock._base.service.integration.pipeline_step import BasePipelineStep
# --- wrapper for stage + worker server ---
# - adapters
# --- input
from tram_analytics.v2.pipeline._mock._base.service.integration.stage_worker_server_bundled.adapters.input_adapters.asyncio_broker import (
    BaseStageAsyncioBrokerInputAdapter
)
# DTOs - for type hints
from tram_analytics.v2.pipeline._mock.common.dto.data_models import EmittedNumber, Square
# --- dependencies ---
# (1) MQ broker(s)
from tram_analytics.v2.pipeline._mock.mq.asyncio_broker.brokers.pipeline_to_stage.broker import \
    PipelineToStagesAsyncioMQBroker
# - output
# MQ to stage
from tram_analytics.v2.pipeline._mock.mq.asyncio_broker.brokers.stage_to_worker_server.adapters.worker_server.output_adapter import \
    WorkerServerAsyncioBrokerOutputAdapter
from tram_analytics.v2.pipeline._mock.mq.asyncio_broker.brokers.stage_to_worker_server.broker import \
    StageToWorkerServerAsyncioMQBroker
# (2) data - inputs (retrieval)
from tram_analytics.v2.pipeline._mock.number_emitter.repository.dict_store.mock_dict_store import \
    EmittedNumberMockDictStore
# --- pipeline stage ---
# (1) adapters
# - input
# ... none for stage -- integrated for stage + worker server -- see below ...
# - output
from tram_analytics.v2.pipeline._mock.squarer.pipeline_stage.adapters.output_adapters.mq.to_pipeline.asyncio_broker import (
    SquarerStageAsyncioBrokerMQToPipelineOutputAdapter
)
# ... MQ to workers ...
from tram_analytics.v2.pipeline._mock.squarer.pipeline_stage.adapters.output_adapters.mq.to_workers.asyncio_broker import (
    SquarerStageAsyncioBrokerMQToWorkersAdapter
)
# (2) stage
from tram_analytics.v2.pipeline._mock.squarer.pipeline_stage.stage import SquarerPipelineStageConfig, \
    SquarerPipelineStage
# (3) data - outputs (storage)
from tram_analytics.v2.pipeline._mock.squarer.repository.dict_store.mock.mock_dict_store import \
    SquarerMockDictStoreConfig, SquarerMockDictStore
# (3) adapters
# - input
from tram_analytics.v2.pipeline._mock.squarer.worker_server.adapters.input_adapters.asyncio_broker import \
    WorkerServerInputAdapter
# data - retrieval
from tram_analytics.v2.pipeline._mock.squarer.worker_server.adapters.output_adapters.repository.read_repo.mock_dict_store import \
    SquarerReadMockDictStore
# data - storage
from tram_analytics.v2.pipeline._mock.squarer.worker_server.adapters.output_adapters.repository.write_repo.mock_dict_store import \
    SquarerWriteMockDictStore
from tram_analytics.v2.pipeline._mock.squarer.worker_server.worker.async_worker.same_thread import \
    SquarerAsyncSameThreadWorker
# --- worker server ---
# (1) worker
from tram_analytics.v2.pipeline._mock.squarer.worker_server.worker.async_worker.separate_thread import \
    SquarerAsyncThreadExecutorWorker
# --- processor ---
from tram_analytics.v2.pipeline._mock.squarer.worker_server.worker.core.squarer import SquarerConfig, Squarer
from tram_analytics.v2.pipeline._mock.squarer.worker_server.worker.sync_worker import SquarerSyncWorker
# (4) server
from tram_analytics.v2.pipeline._mock.squarer.worker_server.worker_server import SquarerWorkerServer
# (2) config
from tram_analytics.v2.pipeline._mock.squarer.worker_server.worker_server import SquarerWorkerServerConfig

# ----- IMPORTS -----

StageAsyncioBrokerInputAdapter: TypeAlias = BaseStageAsyncioBrokerInputAdapter[
    EmittedNumber, Square, SquarerPipelineStageConfig, SquarerWorkerServerConfig
]

# ----- INTEGRATION -----

# worker server adapters
class WorkerServerOutputAdapters(NamedTuple):
    data_read: SquarerReadMockDictStore
    data_write: SquarerWriteMockDictStore
    mq_to_pipeline: WorkerServerAsyncioBrokerOutputAdapter

# pipeline stage adapters
class StageOutputAdapters(NamedTuple):
    to_workers: SquarerStageAsyncioBrokerMQToWorkersAdapter
    to_pipeline: SquarerStageAsyncioBrokerMQToPipelineOutputAdapter

# PIPELINE STEP for a single stream
class BaseSquarerPipelineStep(BasePipelineStep, ABC):

    def __init__(self, *,
                 # broker - pipeline to stage
                 broker: PipelineToStagesAsyncioMQBroker,
                 # source data store
                 src_data_repo: EmittedNumberMockDictStore,
                 # dest data store
                 dest_data_repo_config: SquarerMockDictStoreConfig,
                 # worker
                 # --- configs ---
                 worker_config: SquarerConfig,
                 worker_server_config: SquarerWorkerServerConfig,
                 stage_config: SquarerPipelineStageConfig
                 ):
        self._pipeline_to_stage_broker: PipelineToStagesAsyncioMQBroker = broker
        self._stage_to_workers_broker: StageToWorkerServerAsyncioMQBroker = self._create_stage_to_workers_broker()

        self._src_data_repo: EmittedNumberMockDictStore = src_data_repo
        self._dest_data_repo: SquarerMockDictStore = self._create_dest_data_repo(dest_data_repo_config)

        self._core_processor: Squarer = Squarer(worker_config)
        self._sync_worker: SquarerSyncWorker = SquarerSyncWorker(self._core_processor)
        self._async_worker: BaseUnorderedAsyncWorker[EmittedNumber, Square] = (
            self._create_async_worker()
        )

        self._worker_server_output_adapters: WorkerServerOutputAdapters = (
            self._create_worker_server_output_adapters()
        )
        self._worker_server: SquarerWorkerServer = self._create_worker_server(worker_server_config)
        self._worker_server_input_adapter: WorkerServerInputAdapter = (
            self._create_worker_server_input_adapter()
        )

        self._stage_output_adapters: StageOutputAdapters = (
            self._create_stage_output_adapters()
        )
        self._stage: SquarerPipelineStage = self._create_stage(stage_config)

        self._input_adapter: StageAsyncioBrokerInputAdapter = self._create_wrapper_input_adapter()


    # --- BUILDING ---

    @staticmethod
    def _create_stage_to_workers_broker() -> StageToWorkerServerAsyncioMQBroker:
        return StageToWorkerServerAsyncioMQBroker()

    @staticmethod
    def _create_dest_data_repo(config: SquarerMockDictStoreConfig) -> SquarerMockDictStore:
        return SquarerMockDictStore(config)

    @abstractmethod
    def _create_async_worker(self) -> BaseUnorderedAsyncWorker[EmittedNumber, Square]:
        pass

    def _create_worker_server_output_adapters(self) -> WorkerServerOutputAdapters:
        return WorkerServerOutputAdapters(
            data_read=SquarerReadMockDictStore(self._src_data_repo),
            data_write=SquarerWriteMockDictStore(self._dest_data_repo),
            mq_to_pipeline=WorkerServerAsyncioBrokerOutputAdapter(self._stage_to_workers_broker)
        )

    def _create_worker_server(self, config: SquarerWorkerServerConfig) -> SquarerWorkerServer:
        adapters: WorkerServerOutputAdapters = self._worker_server_output_adapters
        return SquarerWorkerServer(
            read_repo=adapters.data_read,
            write_repo=adapters.data_write,
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
            to_workers=SquarerStageAsyncioBrokerMQToWorkersAdapter(self._stage_to_workers_broker),
            to_pipeline=SquarerStageAsyncioBrokerMQToPipelineOutputAdapter(self._pipeline_to_stage_broker)
        )

    def _create_stage(self, config: SquarerPipelineStageConfig) -> SquarerPipelineStage:
        adapters: StageOutputAdapters = self._stage_output_adapters
        return SquarerPipelineStage(
            config=config,
            mq_to_workers_adapter=adapters.to_workers,
            mq_to_pipeline_adapter=adapters.to_pipeline
        )

    def _create_wrapper_input_adapter(self) -> StageAsyncioBrokerInputAdapter:
        return StageAsyncioBrokerInputAdapter(
            main_broker_queue_to_consume=self._pipeline_to_stage_broker.in_progress.squarer.to_stage,
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

class SquarerSameThreadPipelineStep(BaseSquarerPipelineStep):

    @override
    def _create_async_worker(self) -> SquarerAsyncSameThreadWorker:
        return SquarerAsyncSameThreadWorker(sync_worker=self._sync_worker)

class SquarerThreadExecutorWorkerPipelineStep(BaseSquarerPipelineStep):

    def __init__(self, executor: ThreadPoolExecutor,
                 **kwargs):
        self._executor: ThreadPoolExecutor = executor
        super().__init__(**kwargs)

    @override
    def _create_async_worker(self) -> SquarerAsyncThreadExecutorWorker:
        return SquarerAsyncThreadExecutorWorker(sync_worker=self._sync_worker,
                                                executor=self._executor)