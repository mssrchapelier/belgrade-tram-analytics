from asyncio import Queue
from typing import override

# worker server
from tram_analytics.v2.pipeline._base.worker_servers.base_worker_server import BaseWorkerServerConfig
# stage input adapter
# from src.v1_2.pipeline._base.pipeline_stage.adapters.input_adapters.from_pipeline import BasePipelineStageInputAdapter_Old
from tram_analytics.v2.pipeline._mock._base.service.pipeline_stage.adapters.input_adapters.asyncio_broker import (
    BaseStageInputAdapter
)
# pipeline stage
from tram_analytics.v2.pipeline._mock._base.service.pipeline_stage.base_pipeline_stage import PipelineStage, \
    PipelineStageConfig
# worker server input adapter
from tram_analytics.v2.pipeline._mock._base.service.worker_server.adapters.input_adapters.asyncio_broker import (
    BaseWorkerServerInputAdapter
)
# message objects (for typing)
from tram_analytics.v2.pipeline._mock.common.dto.messages import FrameJobInProgressMessage


# broker (between the stage and the worker server)


# USE:
# For integrating stage + worker server where, for each (per-camera) stage, there is a unique worker server.
# In this case, it is judged best to use a dedicated lightweight broker stand-in between the two
# (currently just asyncio queues-based) and to package the stage, the worker server, and the broker separately.
# The MAIN broker will then talk only to this input port, which simplifies deployment.
#
# NOTE: Whilst the idea of "worker server" was born as a helpful generalisation,
# admittedly in this case this is way too abstracted when the "worker server"
# is in fact an abstraction over a single lightweight stateful object
# performing some calculations.
# However, with this setup, if need be, the stage and the worker server can indeed be decoupled,
# and any adapter for the broker injected (if the two are no longer colocated
# and need to pass messages over e. g. RabbitMQ).

# input adapter:
# - stage
# - worker server
# - (local lightweight) MQ broker between the stage and the worker server
# - (local lightweight) MQ broker between the pipeline and the stage
#
# on startup: start broker, then worker server, then stage
# on shutdown: shut down stage, then worker server, then broker

# THIS version of the input adapter is for the following deployment option:
# the MAIN broker is also asyncio (as opposed to RabbitMQ or anything else).
# Motivation: Full in-process, in-thread communication without reliance on an external MQ broker.

class BaseStageAsyncioBrokerInputAdapter[
    InputT, OutputT,
    PipelineStageConfigT: PipelineStageConfig,
    WorkerServerConfigT: BaseWorkerServerConfig
](
    BaseStageInputAdapter[PipelineStageConfigT]
):

    def __init__(
            self, *,
            main_broker_queue_to_consume: Queue[FrameJobInProgressMessage],
            pipeline_stage: PipelineStage[PipelineStageConfigT],
            worker_server_input_adapter: BaseWorkerServerInputAdapter[
                InputT, OutputT, WorkerServerConfigT
            ]
    ):
        super().__init__(queue_to_consume=main_broker_queue_to_consume,
                         pipeline_stage=pipeline_stage)
        # input adapter to drive the worker server through the asyncio inner broker
        self._worker_server_input_adapter: BaseWorkerServerInputAdapter[
            InputT, OutputT, WorkerServerConfigT
        ] = worker_server_input_adapter

    @override
    async def _on_startup(self) -> None:
        await self._worker_server_input_adapter.start()
        # start pipeline stage the last
        await super()._on_startup()

    @override
    async def _on_shutdown(self) -> None:
        # shutdown pipeline stage the first
        await super()._on_shutdown()
        await self._worker_server_input_adapter.shutdown()
