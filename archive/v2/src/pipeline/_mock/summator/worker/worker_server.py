from typing import TypeAlias

from tram_analytics.v2.pipeline._base.worker_servers.base_worker_server import BaseWorkerServerConfig
from tram_analytics.v2.pipeline._mock.common.dto.data_models import Square, SumSquares
from tram_analytics.v2.pipeline._mock._base.service.worker_server.servers.base_servers.ordered import OrderedWorkerServer

SummatorWorkerServer: TypeAlias = OrderedWorkerServer[Square, SumSquares, BaseWorkerServerConfig]
