from typing import NamedTuple, override

# --- base stream pipeline step ---
from tram_analytics.v2.pipeline._mock._base.service.integration.pipeline_step import BasePipelineStep
# --- dependencies ---
# (1) MQ broker
from tram_analytics.v2.pipeline._mock.mq.asyncio_broker.brokers.pipeline_to_stage.broker import \
    PipelineToStagesAsyncioMQBroker
# (2) data - inputs (retrieval)
# (3) data - outputs (storage)
from tram_analytics.v2.pipeline._mock.number_emitter.repository.dict_store.mock_dict_store import (
    EmittedNumberMockDictStore, EmittedNumberMockDictStoreConfig
)
# (2) adapters
# - output
# MQ to pipeline
from tram_analytics.v2.pipeline._mock.number_emitter.stage.adapters.output_adapters.mq.to_pipeline.asyncio_broker import (
    StageAsyncioBrokerMQOutputAdapter
)
# data - storage
from tram_analytics.v2.pipeline._mock.number_emitter.stage.adapters.output_adapters.repository.dict_repo.mock.mock_dict_store import (
    NumberEmitterWriteMockDictStore
)
# (4) stage
from tram_analytics.v2.pipeline._mock.number_emitter.stage.stage import NumberEmitterStage
# --- stage ---
# (1) config
from tram_analytics.v2.pipeline._mock.number_emitter.stage.stage import NumberEmitterStageConfig


# --- IMPORTS ---
# DTOs - for type hints

# ----- INTEGRATION -----

# stage adapters
class StageOutputAdapters(NamedTuple):
    data_write: NumberEmitterWriteMockDictStore
    mq_to_pipeline: StageAsyncioBrokerMQOutputAdapter

# PIPELINE STEP for a single stream
class NumberEmitterPipelineStep(BasePipelineStep):
    def __init__(self, *,
                 stage_seed: int,  # for the RNG for frame id generation (repeatability for mocking purposes)
                 broker: PipelineToStagesAsyncioMQBroker,
                 dest_data_repo_config: EmittedNumberMockDictStoreConfig,
                 stage_config: NumberEmitterStageConfig
                 ) -> None:
        self._pipeline_to_stage_broker: PipelineToStagesAsyncioMQBroker = broker

        self._dest_data_repo: EmittedNumberMockDictStore = self._create_dest_data_repo(
            dest_data_repo_config
        )
        self._output_adapters: StageOutputAdapters = self._create_output_adapters()
        self._stage: NumberEmitterStage = self._create_stage(stage_config, stage_seed)

    # --- BUILDING ---

    @staticmethod
    def _create_dest_data_repo(config: EmittedNumberMockDictStoreConfig) -> EmittedNumberMockDictStore:
        return EmittedNumberMockDictStore(config)

    def _create_output_adapters(self) -> StageOutputAdapters:
        return StageOutputAdapters(
            data_write=NumberEmitterWriteMockDictStore(self._dest_data_repo),
            mq_to_pipeline=StageAsyncioBrokerMQOutputAdapter(self._pipeline_to_stage_broker)
        )

    def _create_stage(self, stage_config: NumberEmitterStageConfig,
                      seed: int) -> NumberEmitterStage:
        return NumberEmitterStage(seed=seed,
                                  config=stage_config,
                                  read_repo=self._output_adapters.data_write,
                                  write_repo=self._output_adapters.mq_to_pipeline)

    # --- BEHAVIOUR ---

    @override
    async def start(self) -> None:
        await self._stage.start()

    @override
    async def shutdown(self) -> None:
        await self._stage.shutdown()