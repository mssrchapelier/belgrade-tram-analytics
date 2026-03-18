from enum import StrEnum, auto

from pydantic import BaseModel

from common.utils.time_utils import get_datetime_utc
from tram_analytics.v2.pipeline._base.models.message import (
    BaseFrameJobInProgressMessage, BaseCriticalErrorMessage, BaseDroppedJobMessage, BaseIngestionDroppedItemMessage
)


class FrameJobInProgressMessage(BaseFrameJobInProgressMessage):
    pass

class CriticalErrorMessage(BaseCriticalErrorMessage):
    pass

class DroppedJobMessage(BaseDroppedJobMessage):
    pass

class IngestionDroppedItemMessage(BaseIngestionDroppedItemMessage):
    pass


class ControlCommand(StrEnum):
    START = auto()
    STOP = auto()


class ControlMessage(BaseModel):
    command: ControlCommand


class ComponentStatus(StrEnum):
    RUNNING = auto()
    STOPPED = auto()


class StatusMessage(BaseModel):
    status: ComponentStatus
    timestamp: float = get_datetime_utc().timestamp()
