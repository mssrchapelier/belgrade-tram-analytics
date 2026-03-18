from typing import NamedTuple, override

# connection
from aio_pika.abc import AbstractRobustConnection

from tram_analytics.v2.pipeline._base.worker_servers.base_worker_server import BaseWorkerServerConfig
# stage input adapter - job requests (RabbitMQ)
from tram_analytics.v2.pipeline._mock._base.service.pipeline_stage.adapters.input_adapters.rabbitmq import (
    StageJobRequestInputAdapter
)
# worker server input adapter (asyncio broker)
from tram_analytics.v2.pipeline._mock._base.service.worker_server.adapters.input_adapters.asyncio_broker import (
    BaseWorkerServerInputAdapter
)
# abstract adapter to subclass - control and status messages (RabbitMQ)
from tram_analytics.v2.pipeline._mock.mq.rabbitmq.base.control_and_status_helper import (
    BaseControlAndStatusInputAdapter, RabbitMQControlAndStatusReportWrapperConfig
)


# --- wrapped instance to manage ---
class ManagedModule[
    InputT, OutputT, WorkerServerConfigT: BaseWorkerServerConfig
](NamedTuple):
    stage: StageJobRequestInputAdapter
    worker_server: BaseWorkerServerInputAdapter[
        InputT, OutputT, WorkerServerConfigT
    ]

# --- input adapter ---
class StageInputAdapter[
    InputT, OutputT, WorkerServerConfigT: BaseWorkerServerConfig
](BaseControlAndStatusInputAdapter[
    ManagedModule[InputT, OutputT, WorkerServerConfigT]
]):
    def __init__(self, *,
                 stage: StageJobRequestInputAdapter,
                 worker_server: BaseWorkerServerInputAdapter[InputT, OutputT, WorkerServerConfigT],
                 connection: AbstractRobustConnection,
                 config: RabbitMQControlAndStatusReportWrapperConfig
                 ) -> None:
        wrapped_client: ManagedModule[
            InputT, OutputT, WorkerServerConfigT
        ] = ManagedModule(stage, worker_server)
        super().__init__(wrapped_client=wrapped_client,
                         connection=connection,
                         config=config)

    @override
    def _get_is_running_wrapped_client(self, wrapped_client: ManagedModule[
        InputT, OutputT, WorkerServerConfigT
    ]) -> bool:
        # TODO: a more semantically robust way to get the running/stopped status of the wrapped client?
        return wrapped_client.stage.is_running and wrapped_client.worker_server.is_running

    @override
    async def _start_wrapped_client(self, wrapped_client: ManagedModule[
        InputT, OutputT, WorkerServerConfigT
    ]) -> None:
        # first the worker server (i. e. the inner part) ...
        await wrapped_client.worker_server.start()
        # ... then the stage (i. e. the outer part)
        await wrapped_client.stage.start()

    @override
    async def _stop_wrapped_client(self, wrapped_client: ManagedModule[
        InputT, OutputT, WorkerServerConfigT
    ]) -> None:
        # first the stage (i. e. the outer part) ...
        await wrapped_client.stage.stop()
        # ... then the worker server (i. e. the inner part)
        await wrapped_client.worker_server.shutdown()
