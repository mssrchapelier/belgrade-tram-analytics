from typing import NamedTuple, override
from enum import StrEnum, auto

# ----- IMPORTS -----

# (solely for type hints)
from tram_analytics.v2.pipeline._base.worker_servers.workers.async_workers.base_async_workers import BaseUnorderedAsyncWorker
from tram_analytics.v2.pipeline._mock.common.dto.data_models import EmittedNumber, Square

# --- dependencies ---
# (1) MQ broker(s)
from src.v1_2.pipeline._mock.mq.asyncio_broker.pipeline_to_stages.pipeline_to_stages import (
    PipelineToStagesAsyncioMQBroker
)
from src.v1_2.pipeline._mock.mq.asyncio_broker.stage_to_worker_server.stage_to_worker_server import (
    StageToWorkerServerAsyncioMQBroker
)
# (2) persistence - inputs (retrieval)
from src.v1_2.pipeline._mock.number_emitter.persistence.dict_persistence.mock_with_delay import EmittedNumberMockWithDelayDictStore
# (3) persistence - outputs (storage)
from tram_analytics.v2.pipeline._mock.squarer.repository.dict_store.mock.mock_dict_store import SquarerMockDictStoreConfig, SquarerMockDictStore

# --- processor ---
from tram_analytics.v2.pipeline._mock.squarer.worker_server.worker.core.squarer import SquarerConfig, Squarer

# --- worker server ---
# (1) worker
from tram_analytics.v2.pipeline._mock.squarer.worker_server.worker.async_worker.separate_thread import SquarerAsyncThreadExecutorWorker
from tram_analytics.v2.pipeline._mock.squarer.worker_server.worker.async_worker.same_thread import SquarerAsyncSameThreadWorker
from tram_analytics.v2.pipeline._mock.squarer.worker_server.worker.sync_worker import SquarerSyncWorker
# (2) config
from tram_analytics.v2.pipeline._base.worker_servers.base_worker_server import BaseWorkerServerConfig
# (3) adapters
from archive.v2.src.pipeline._mock.squarer.worker_server.adapters._old.output_to_mq import SquarerWorkerMockMQBrokerOutputAdapter
from tram_analytics.v2.pipeline._mock.squarer.worker_server.adapters.output_adapters.repository.write_repo.mock_dict_store import \
    SquarerWriteMockDictStore
from tram_analytics.v2.pipeline._mock.squarer.worker_server.adapters.output_adapters.repository.read_repo.mock_dict_store import \
    SquarerReadMockDictStore
from src.v1_2.pipeline._mock.mq.asyncio_broker.stage_to_worker_server.input_adapter import (
    WorkerServerAsyncioBrokerInputAdapter
)
# (4) server
from tram_analytics.v2.pipeline._mock.squarer.worker_server.worker_server import SquarerWorkerServer

# --- pipeline stage ---
# (1) config
from tram_analytics.v2.pipeline._base.pipeline_stage.base_pipeline_stage import BasePipelineStageConfig
# (2) adapters
from tram_analytics.v2.pipeline._mock.squarer.pipeline_stage.adapters.output_adapters.mq.to_workers.asyncio_broker import (
    SquarerStageToWorkersMQBrokerOutputAdapter
)
from tram_analytics.v2.pipeline._mock.squarer.pipeline_stage.adapters.output_adapters.mq.to_pipeline.asyncio_broker import (
    SquarerStageAsyncioBrokerMQToPipelineOutputAdapter
)
# (3) stage
from tram_analytics.v2.pipeline._mock._base.service.pipeline_stage.base_pipeline_stage import PipelineStage

# --- wrapper for stage + worker server ---
# from src.v1_2.pipeline._mock._base.integration.service.stage_with_own_worker_server.input_adapters.mq.asyncio_broker import (
    # StageInputAdapter_Old,
    # StageInputAdapter
# )
from tram_analytics.v2.pipeline._mock._base.service.integration.stage_worker_server_bundled.adapters.input_adapters.asyncio_broker import (
    BaseStageAsyncioBrokerInputAdapter
)

# --- service ---
from tram_analytics.v2.pipeline._mock._base.service.integration.pipeline_step import BasePipelineStep


# ----- INTEGRATION -----

# worker server adapters
class SquarerWorkerServerOutputAdapters(NamedTuple):
    data_retrieval: SquarerReadMockDictStore
    data_persistence: SquarerWriteMockDictStore
    mq_to_pipeline: SquarerWorkerMockMQBrokerOutputAdapter

# pipeline stage adapters
class SquarerStageOutputAdapters(NamedTuple):
    to_workers: SquarerStageToWorkersMQBrokerOutputAdapter
    to_pipeline: SquarerStageAsyncioBrokerMQToPipelineOutputAdapter

# SERVICE
class SquarerService(BasePipelineStep):

    class WorkerThreadingOption(StrEnum):
        SAME_THREAD = auto()
        SEPARATE_SINGLE_THREAD = auto()

    def __init__(self, *,
                 # --- injected dependencies ---
                 broker: PipelineToStagesAsyncioMQBroker,
                 retrieval_persistence: EmittedNumberMockWithDelayDictStore,
                 # --- deployment options ---
                 worker_threading_option: WorkerThreadingOption,
                 # --- configs ---
                 persistence_config: SquarerMockDictStoreConfig,
                 worker_config: SquarerConfig,
                 worker_server_config: BaseWorkerServerConfig,
                 stage_config: BasePipelineStageConfig
                 ):
        self._pipeline_to_stage_broker: PipelineToStagesAsyncioMQBroker = broker
        self._stage_to_workers_broker: StageToWorkerServerAsyncioMQBroker = self._create_stage_to_workers_broker()

        self._retrieval_persistence: EmittedNumberMockWithDelayDictStore = retrieval_persistence

        self._persistence: SquarerMockDictStore = self._create_persistence(persistence_config)

        self._processor: Squarer = Squarer(worker_config)
        self._sync_worker: SquarerSyncWorker = SquarerSyncWorker(self._processor)
        self._worker: BaseUnorderedAsyncWorker[EmittedNumber, Square] = (
            self._create_async_worker(worker_threading_option)
        )

        self._worker_server_output_adapters: SquarerWorkerServerOutputAdapters = (
            self._create_output_adapters_for_worker_server()
        )
        self._worker_server: SquarerWorkerServer = self._create_worker_server(worker_server_config)
        self._worker_server_input_adapter: WorkerServerAsyncioBrokerInputAdapter[
            EmittedNumber, Square, BaseWorkerServerConfig
        ] = (
            self._create_input_adapter_for_worker_server()
        )

        self._stage_output_adapters: SquarerStageOutputAdapters = (
            self._create_output_adapters_for_stage()
        )
        self._stage: PipelineStage = self._create_stage(stage_config)
        # self._stage_input_adapter: SquarerStageMockPipelineStageAsyncioInputAdapter = (
        #     self._create_input_adapter_for_stage()
        # )

        self._wrapper_input_adapter: BaseStageAsyncioBrokerInputAdapter[
            EmittedNumber, Square, BaseWorkerServerConfig
        ] = self._create_wrapper_input_adapter()


    # --- BUILDING ---

    @staticmethod
    def _create_stage_to_workers_broker() -> StageToWorkerServerAsyncioMQBroker:
        return StageToWorkerServerAsyncioMQBroker()

    def _create_async_worker(
            self, option: WorkerThreadingOption
    ) -> BaseUnorderedAsyncWorker[EmittedNumber, Square]:
        match option:
            case SquarerService.WorkerThreadingOption.SAME_THREAD:
                return SquarerAsyncSameThreadWorker(self._sync_worker)
            case SquarerService.WorkerThreadingOption.SEPARATE_SINGLE_THREAD:
                return SquarerAsyncThreadExecutorWorker(self._sync_worker)
            case _:
                raise ValueError(f"Unknown option: {option}")

    @staticmethod
    def _create_persistence(config: SquarerMockDictStoreConfig) -> SquarerMockDictStore:
        return SquarerMockDictStore(config)

    def _create_output_adapters_for_worker_server(self) -> SquarerWorkerServerOutputAdapters:
        return SquarerWorkerServerOutputAdapters(
            data_retrieval=SquarerReadMockDictStore(self._retrieval_persistence),
            data_persistence=SquarerWriteMockDictStore(self._persistence),
            mq_to_pipeline=SquarerWorkerMockMQBrokerOutputAdapter(self._stage_to_workers_broker)
        )

    def _create_worker_server(self, config: BaseWorkerServerConfig) -> SquarerWorkerServer:
        adapters: SquarerWorkerServerOutputAdapters = self._worker_server_output_adapters
        return SquarerWorkerServer(
            data_retrieval_adapter=adapters.data_retrieval,
            data_persistence_adapter=adapters.data_persistence,
            mq_to_pipeline_adapter=adapters.mq_to_pipeline,
            config=config,
            worker=self._worker
        )

    def _create_input_adapter_for_worker_server(self) -> WorkerServerAsyncioBrokerInputAdapter[
            EmittedNumber, Square, BaseWorkerServerConfig
    ]:
        return WorkerServerAsyncioBrokerInputAdapter(
            queue_to_consume=self._stage_to_workers_broker.stage_to_workers,
            worker_server=self._worker_server
        )

    def _create_output_adapters_for_stage(self) -> SquarerStageOutputAdapters:
        return SquarerStageOutputAdapters(
            to_workers=SquarerStageToWorkersMQBrokerOutputAdapter(self._stage_to_workers_broker),
            to_pipeline=SquarerStageAsyncioBrokerMQToPipelineOutputAdapter(self._pipeline_to_stage_broker)
        )

    def _create_stage(self, config: BasePipelineStageConfig) -> PipelineStage:
        adapters: SquarerStageOutputAdapters = self._stage_output_adapters
        return PipelineStage(
            config=config,
            mq_to_workers_adapter=adapters.to_workers,
            mq_to_pipeline_adapter=adapters.to_pipeline
        )

    # def _create_input_adapter_for_stage(self) -> SquarerStageMockPipelineStageAsyncioInputAdapter:
    #     return SquarerStageMockPipelineStageAsyncioInputAdapter(
    #         broker=self._pipeline_to_stage_broker,
    #         stage=self._stage
    #     )

    def _create_wrapper_input_adapter(self) -> BaseStageAsyncioBrokerInputAdapter[
        EmittedNumber, Square, BaseWorkerServerConfig
    ]:
        return BaseStageAsyncioBrokerInputAdapter(
            main_broker_queue_to_consume=self._pipeline_to_stage_broker.in_progress.squarer.to_stage,
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