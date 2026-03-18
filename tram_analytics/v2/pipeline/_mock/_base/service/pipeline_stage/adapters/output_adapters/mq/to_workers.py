from abc import ABC

from tram_analytics.v2.pipeline._base.pipeline_stage.adapters.output_adapters.mq.to_workers import \
    StageMQToWorkersOutputPort as BasePort
from tram_analytics.v2.pipeline._mock.common.dto.messages import FrameJobInProgressMessage


class StageMQToWorkersOutputPort(
    BasePort[FrameJobInProgressMessage, FrameJobInProgressMessage],
    ABC
):
    pass