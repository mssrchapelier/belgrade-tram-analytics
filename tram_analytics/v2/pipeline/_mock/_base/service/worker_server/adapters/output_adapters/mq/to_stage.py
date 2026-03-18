from abc import ABC

from tram_analytics.v2.pipeline._base.worker_servers.adapters.output_adapters.mq.to_stage import (
    BaseWorkerServerMQOutputPort as BasePort
)
from tram_analytics.v2.pipeline._mock.common.dto.messages import FrameJobInProgressMessage, CriticalErrorMessage


class BaseWorkerServerMQOutputPort(
    BasePort[FrameJobInProgressMessage, CriticalErrorMessage],
    ABC
):
    pass