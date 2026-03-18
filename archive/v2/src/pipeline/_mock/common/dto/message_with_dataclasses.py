from dataclasses import dataclass

from archive.v2.src.pipeline._base.models.message_with_dataclasses import (
    BaseFrameJobInProgressMessage, BaseCriticalErrorMessage, BaseDroppedJobMessage, BaseIngestionDroppedItemMessage
)

@dataclass(frozen=True, slots=True, kw_only=True)
class FrameJobInProgressMessage(BaseFrameJobInProgressMessage):
    pass

@dataclass(frozen=True, slots=True, kw_only=True)
class CriticalErrorMessage(BaseCriticalErrorMessage):
    pass

@dataclass(frozen=True, slots=True, kw_only=True)
class DroppedJobMessage(BaseDroppedJobMessage):
    pass

@dataclass(frozen=True, slots=True, kw_only=True)
class IngestionDroppedItemMessage(BaseIngestionDroppedItemMessage):
    pass