from typing import NamedTuple, override, AsyncIterator

from tram_analytics.v2.pipeline._mock._base.service.integration.pipeline_step import BasePipelineStep
from tram_analytics.v2.pipeline._mock.common.dto.data_models import NumberObservation
from src.v1_2.pipeline._mock.mq.asyncio_broker.pipeline_to_stages.pipeline_to_stages import PipelineToStagesAsyncioMQBroker
from src.v1_2.pipeline._mock.number_emitter.persistence.dict_persistence.mock_with_delay import EmittedNumberMockWithDelayDictStore, EmittedNumberMockWithDelayDictPersistenceConfig
from src.v1_2.pipeline._mock.number_emitter.pipeline_stage.adapters.mq_adapter import (
    NumberEmitterStageToPipelineMockMQBrokerOutputAdapter
)
from src.v1_2.pipeline._mock.number_emitter.pipeline_stage.adapters.persistence_adapter import (
    EmittedNumberDataPersistenceAdapter
)
from src.v1_2.pipeline._mock.number_emitter.pipeline_stage.pipeline_stage import NumberEmitterStage, NumberEmitterStageConfig

class NumberEmitterStageOutputAdapters(NamedTuple):
    data_persistence: EmittedNumberDataPersistenceAdapter
    mq_to_pipeline: NumberEmitterStageToPipelineMockMQBrokerOutputAdapter

class NumberEmitterService(BasePipelineStep):

    def __init__(self, *,
                 stage_seed: int,  # for the RNG (repeatability for mock purposes)
                 broker: PipelineToStagesAsyncioMQBroker,
                 src_stream: AsyncIterator[NumberObservation],
                 persistence_config: EmittedNumberMockWithDelayDictPersistenceConfig,
                 stage_config: NumberEmitterStageConfig
                 ) -> None:
        self._broker: PipelineToStagesAsyncioMQBroker = broker

        self._persistence: EmittedNumberMockWithDelayDictStore = self._create_persistence(persistence_config)
        self._output_adapters: NumberEmitterStageOutputAdapters = self._create_output_adapters()
        self._stage: NumberEmitterStage = self._create_stage(stage_config, src_stream, stage_seed)

    # --- BUILDING ---

    def _create_persistence(self, persistence_config: EmittedNumberMockWithDelayDictPersistenceConfig) -> EmittedNumberMockWithDelayDictStore:
        return EmittedNumberMockWithDelayDictStore(persistence_config)

    def _create_output_adapters(self) -> NumberEmitterStageOutputAdapters:
        return NumberEmitterStageOutputAdapters(
            data_persistence=EmittedNumberDataPersistenceAdapter(self._persistence),
            mq_to_pipeline=NumberEmitterStageToPipelineMockMQBrokerOutputAdapter(self._broker)
        )

    def _create_stage(self, stage_config: NumberEmitterStageConfig, src_stream: AsyncIterator[NumberObservation],
                      seed: int):
        return NumberEmitterStage(seed=seed,
                                  config=stage_config,
                                  src_stream=src_stream,
                                  data_persistence_adapter=self._output_adapters.data_persistence,
                                  mq_to_pipeline_adapter=self._output_adapters.mq_to_pipeline)

    # --- BEHAVIOUR ---

    @override
    async def start(self) -> None:
        await self._stage.start()

    @override
    async def shutdown(self) -> None:
        await self._stage.shutdown()
