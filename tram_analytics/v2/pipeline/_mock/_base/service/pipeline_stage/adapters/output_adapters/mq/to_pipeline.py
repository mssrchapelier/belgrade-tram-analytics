from abc import ABC

from tram_analytics.v2.pipeline._base.mq.base_mq import BaseOutputMQMultiChannel
from tram_analytics.v2.pipeline._mock.common.dto.messages import (
    FrameJobInProgressMessage, DroppedJobMessage, CriticalErrorMessage
)


class BaseStageMQToPipelineOutputPort(
    BaseOutputMQMultiChannel[FrameJobInProgressMessage, DroppedJobMessage, CriticalErrorMessage],
    ABC
):
    pass