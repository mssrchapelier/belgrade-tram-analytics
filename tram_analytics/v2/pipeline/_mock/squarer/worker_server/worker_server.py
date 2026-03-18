from typing import TypeAlias

from tram_analytics.v2.pipeline._base.worker_servers.base_worker_server import BaseWorkerServerConfig
from tram_analytics.v2.pipeline._mock._base.service.worker_server.servers.base_servers.unordered import \
    UnorderedWorkerServer
from tram_analytics.v2.pipeline._mock.common.dto.data_models import EmittedNumber, Square


class SquarerWorkerServerConfig(BaseWorkerServerConfig):
    pass

SquarerWorkerServer: TypeAlias = UnorderedWorkerServer[EmittedNumber, Square, SquarerWorkerServerConfig]