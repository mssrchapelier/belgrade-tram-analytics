from tram_analytics.v2.pipeline._mock._base.service.worker_server.adapters.input_adapters.asyncio_broker import (
    BaseWorkerServerInputAdapter
)
from tram_analytics.v2.pipeline._mock.common.dto.data_models import Square, SumSquares
from tram_analytics.v2.pipeline._mock.summator.worker_server.worker_server import SummatorWorkerServerConfig


class WorkerServerInputAdapter(
    BaseWorkerServerInputAdapter[Square, SumSquares, SummatorWorkerServerConfig]
):
    pass