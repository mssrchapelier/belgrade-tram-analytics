from asyncio import Queue
from typing import override

from tram_analytics.v2.pipeline._base.pipeline_stage.base_pipeline_stage import BasePipelineStage
from tram_analytics.v2.pipeline._base.worker_servers.base_worker_server import BaseWorkerServerConfig, BaseWorkerServer
from tram_analytics.v2.pipeline._mock._base.service.pipeline_stage.adapters.input_adapters.asyncio_broker import \
    BaseStageInputAdapter
from tram_analytics.v2.pipeline._mock._base.service.worker_server.adapters.input_adapters.asyncio_broker import \
    BaseWorkerServerInputAdapter
from tram_analytics.v2.pipeline._mock.common.dto.messages import FrameJobInProgressMessage, DroppedJobMessage, CriticalErrorMessage
from tram_analytics.v2.pipeline._mock.mq.asyncio_broker.brokers.stage_to_worker_server.broker import StageToWorkerServerAsyncioMQBroker


class StageInputAdapter_Old[
    InputT, OutputT, WorkerServerConfigT: BaseWorkerServerConfig
](
    BaseStageInputAdapter[
        FrameJobInProgressMessage, DroppedJobMessage, CriticalErrorMessage
    ]
):

    def __init__(
            self, *,
            main_broker_queue_to_consume: Queue[FrameJobInProgressMessage],
            pipeline_stage: BasePipelineStage[
                FrameJobInProgressMessage, DroppedJobMessage, CriticalErrorMessage
            ],
            # the broker connecting the pipeline stage and the worker server
            inner_broker: StageToWorkerServerAsyncioMQBroker,
            worker_server: BaseWorkerServer[
                FrameJobInProgressMessage, FrameJobInProgressMessage,
                CriticalErrorMessage, InputT, OutputT, WorkerServerConfigT
            ]
    ):
        super().__init__(queue_to_consume=main_broker_queue_to_consume,
                         pipeline_stage=pipeline_stage)
        self._worker_server: BaseWorkerServer[
            FrameJobInProgressMessage, FrameJobInProgressMessage,
            CriticalErrorMessage, InputT, OutputT, WorkerServerConfigT
        ] = worker_server
        self._inner_broker: StageToWorkerServerAsyncioMQBroker = inner_broker
        # input adapter to drive the worker server through the asyncio inner broker
        self._worker_server_input_adapter: BaseWorkerServerInputAdapter[
            InputT, OutputT, WorkerServerConfigT
        ] = BaseWorkerServerInputAdapter(
            queue_to_consume=self._inner_broker.stage_to_workers,
            worker_server=worker_server
        )

    @override
    async def _on_startup(self) -> None:
        await self._start_inner_broker()
        await self._worker_server_input_adapter.start()
        # start pipeline stage the last
        await super()._on_startup()

    @override
    async def _on_shutdown(self) -> None:
        # shutdown pipeline stage the first
        await super()._on_shutdown()
        await self._worker_server_input_adapter.shutdown()
        await self._shutdown_inner_broker()

    # NOTE: Start and shutdown are not implemented for the asyncio queue-based broker being used.
    # If it is replaced with one that needs to be shut down, implement the functionality
    # in `_start_broker`, `_shutdown_broker`.

    async def _start_inner_broker(self) -> None:
        pass

    async def _shutdown_inner_broker(self) -> None:
        pass
