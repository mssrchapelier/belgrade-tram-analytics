from tram_analytics.v2.pipeline._mock._base.service.worker_server.adapters.input_adapters.asyncio_broker import (
    BaseWorkerServerInputAdapter
)
from tram_analytics.v2.pipeline._mock.common.dto.data_models import EmittedNumber, Square
from tram_analytics.v2.pipeline._mock.squarer.worker_server.worker_server import SquarerWorkerServerConfig


class WorkerServerInputAdapter(
    BaseWorkerServerInputAdapter[EmittedNumber, Square, SquarerWorkerServerConfig]
):
    pass