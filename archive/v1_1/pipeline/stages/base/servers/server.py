from typing import override
from abc import ABC, abstractmethod

from pydantic import BaseModel

from common.utils.data_structures.keyed_ring_buffer import KeyedRingBuffer
from archive.v1_1.pipeline.stages.base.message import FrameMessage

class BaseProcessorConfig(BaseModel, ABC):
    pass

class BaseProcessor[InputT, OutputT, ConfigT: BaseProcessorConfig](ABC):

    """
    The base abstraction for any processing module operating on raw inputs and outputs.
    Can be stateful/stateless, camera-dependent/camera-agnostic, etc.
    """

    def __init__(self, config: ConfigT):
        self._config: ConfigT = config

    @abstractmethod
    async def predict(self, input_item: InputT) -> OutputT:
        pass

class LoggingParams(BaseModel):
    log_path: str
    level: int

class BasePersistenceConfig(BaseModel, ABC):
    logging: LoggingParams

class BaseProcessorServer[
    InputT, OutputT,  # input, output type
    ConfigT: BaseProcessorConfig, PersistenceConfigT: BasePersistenceConfig,
    PredictionRequestT: FrameMessage, PredictionResponseT: FrameMessage,  # request, response type for prediction
    RealtimeRetrievalRequestT, RealtimeRetrievalResponseT # request, response type for retrieval
](ABC):

    """
    A wrapper around a processor that exposes two endpoints for real-time pipeline use:
        - `.predict(...)` for predictions (request-response);
        - `.retrieve(...)` to retrieve the stored outputs.
    It is intended for the predict response to be lightweight (although not necessarily so);
    the outputs themselves can be retrieved using `.retrieve(...)`
    This is done to decouple message passing between modules
    from transferring raw data (e. g. inference results),
    which may be large and best served through a different means
    (possibly retrieved from Redis, shared memory on the same node, etc.)
    """

    def __init__(self, *, processor: BaseProcessor[InputT, OutputT, ConfigT],
                 persistence_config: PersistenceConfigT) -> None:
        self._processor: BaseProcessor[InputT, OutputT, ConfigT] = processor
        self._config: PersistenceConfigT = persistence_config

    async def predict(self, request: PredictionRequestT) -> PredictionResponseT:
        # get the input item using the request
        input_item: InputT = await self._retrieve_inputs(request)
        # send to the worker, get the result
        result: OutputT = await self._processor.predict(input_item)
        # store the result
        await self._persist_result(request, result)
        # build the response
        response: PredictionResponseT = await self._build_response(request, result)
        return response

    @abstractmethod
    async def _retrieve_inputs(self, request: PredictionRequestT) -> InputT:
        """
        Get inputs to pass to the processor using data in the prediction request.
        """
        pass

    @abstractmethod
    async def _persist_result(self, prediction_request: PredictionRequestT, result: OutputT) -> None:
        """
        Store the prediction result (using also the necessary data from the request).
        """
        pass

    @abstractmethod
    async def _build_response(self, request: PredictionRequestT, result: OutputT) -> PredictionResponseT:
        """
        Build a response for the prediction request (using the result and the request).
        """
        pass

    @abstractmethod
    async def retrieve_for_realtime(self, request: RealtimeRetrievalRequestT) -> RealtimeRetrievalResponseT:
        """
        Retrieve stored prediction results.
        """
        pass

class BaseKeyedRingBufferPersistenceClient[OutputT](ABC):
    """
    Uses a simple ring buffer with fast key access
    (backed by a dictionary and a deque with max length set)
    for persistence; keyed by frame ID as a unique identifier.
    """

    def __init__(self, max_size: int) -> None:
        # frame id -> output
        self._buffer: KeyedRingBuffer[str, OutputT] = KeyedRingBuffer(max_size=max_size)

    async def store(self, frame_id: str, result: OutputT) -> None:
        self._buffer.upsert(key=frame_id, value=result)

# TODO: remove
class BaseProcessorServerWithKeyedRingBuffer[
    InputT, OutputT,  # input, output type
    ConfigT: BaseProcessorConfig, PersistenceConfigT: BasePersistenceConfig,
    PredictionRequestT: FrameMessage, PredictionResponseT: FrameMessage,  # request, response type for prediction
    RealtimeRetrievalRequestT, RealtimeRetrievalResponseT # request, response type for retrieval
](
    BaseProcessorServer[InputT, OutputT, ConfigT, PersistenceConfigT, PredictionRequestT, PredictionResponseT,
                        RealtimeRetrievalRequestT, RealtimeRetrievalResponseT],
    ABC
):

    def __init__(self, *, processor: BaseProcessor[InputT, OutputT, ConfigT],
                 persistence_config: PersistenceConfigT,
                 persistence_client: BaseKeyedRingBufferPersistenceClient[OutputT]) -> None:
        super().__init__(processor=processor, persistence_config=persistence_config)
        self._persistence_client: BaseKeyedRingBufferPersistenceClient[OutputT] = persistence_client

    @override
    async def _persist_result(self, prediction_request: FrameMessage, result: OutputT) -> None:
        await self._persistence_client.store(frame_id=prediction_request.frame_id,
                                             result=result)

