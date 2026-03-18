from typing import override
from abc import ABC, abstractmethod
from archive.v1_1.sandbox._old_2.base_processors import (
    BaseProcessorConfig, BaseAsyncProcessor
)

from pydantic import BaseModel

# --- models ---

class Frame:
    pass

class Detections:
    pass

class FeatureVectors:
    pass

class TrackingInfo:
    pass

class DerivedVehicleInfo:
    pass

class SceneEvents:
    pass

class LiveState:
    pass

# --- detections ---

class BaseDetectionsWorkerConfig(BaseProcessorConfig):
    pass

class BaseDetectionsWorker[ConfigT: BaseDetectionsWorkerConfig](
    BaseAsyncProcessor[ConfigT, Frame, Detections],
    ABC
):
    pass

class YOLODetectionsWorkerConfig(BaseDetectionsWorkerConfig):
    pass

class YOLODetectionsAsyncWorker(BaseDetectionsWorker[YOLODetectionsWorkerConfig]):

    @override
    async def process_for_frame(self, inputs: Frame) -> Detections:
        raise NotImplementedError()

class BasePoolingProcessorConfig(BaseModel):
    pass

class BasePoolingProcessor[
    BasicConfigT: BaseProcessorConfig,
    PoolingConfigT: BasePoolingProcessorConfig,
    InputT, OutputT
](ABC):

    def __init__(self, basic_config: BasicConfigT, pooling_config: PoolingConfigT) -> None:
        self._processor: BaseAsyncProcessor[BasicConfigT, InputT, OutputT] = (
            self._get_inner_processor(basic_config)
        )
        self._config: PoolingConfigT = pooling_config

    @classmethod
    @abstractmethod
    def _get_inner_processor(cls, config: BasicConfigT) -> BaseAsyncProcessor[BasicConfigT, InputT, OutputT]:
        pass

