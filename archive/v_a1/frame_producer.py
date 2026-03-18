from typing import List, Dict, Generator, AsyncGenerator
import asyncio
from asyncio.tasks import Task
from abc import ABC, abstractmethod

from pydantic import BaseModel

from archive.v_a1.item_stream import ItemStream

class FrameItemRaw(BaseModel):
    camera_id: str
    image: bytes
    timestamp: str

class SynchronousFrameEmitter(ABC):

    def __init__(self, link: str, camera_id: str):
        self._link: str = link
        self._camera_id: str = camera_id

    @abstractmethod
    def __iter__(self) -> FrameItemRaw:
        pass

class AsyncFrameEmitter:

    def __init__(self, sync_emitter: SynchronousFrameEmitter):
        self.sync_emitter: SynchronousFrameEmitter = sync_emitter

    def _get_sync_generator(self) -> Generator[FrameItemRaw]:
        for item in self.sync_emitter: # type: FrameItemRaw
            yield item

    async def __aiter__(self) -> AsyncGenerator[FrameItemRaw]:
        loop = asyncio.get_running_loop()
        sync_gen: Generator[FrameItemRaw] = self._get_sync_generator()
        while True:
            item: FrameItemRaw = await loop.run_in_executor(
                None, next, sync_gen
            )
            yield item

class FrameProducingService:

    """
    Run several blocking frame emitters concurrently and publish all frames to a common stream.
    After initialising this class, call add_producer for each producer to connect it to the stream.
    Iterate the instance to get frames from the stream.
    """

    def __init__(self):
        # ... initialise configuration ...

        # PRODUCERS: After initialising FrameProducer, call add_producer() for each camera.
        # { camera_id: CameraStreamer }
        self._producers: Dict[str, AsyncFrameEmitter] = dict()
        # all producers publish into the same stream -- change this logic later if needed
        self._stream: ItemStream[FrameItemRaw] = ItemStream()

        # PRODUCER TASKS: managed by add_producer() / remove_producer()
        # { camera_id: Task }
        self._producer_tasks: Dict[str, Task] = dict()

    def add_producer(self, camera_id: str, producer: AsyncFrameEmitter) -> None:
        """
        Adds producer to this instance and connects it to the common stream.
        """
        if camera_id in self._producers:
            raise ValueError(f"Producer ID {camera_id} is already registered.")
        self._producers[camera_id] = producer
        self._create_producer_task(camera_id)

    def remove_producer(self, camera_id: str) -> None:
        """
        Disconnects this producer from the common stream and removes it from this instance.
        """
        if camera_id not in self._producers:
            raise ValueError(f"Can't remove producer {camera_id} from stream: ID not registered.")
        if camera_id not in self._producer_tasks:
            raise ValueError(f"Producer {camera_id} already disconnected from the stream.")
        task: Task = self._producer_tasks[camera_id]
        if not task.cancelled():
            task.cancel()
        self._producer_tasks.pop(camera_id)
        self._producers.pop(camera_id)

    async def get_camera_ids(self) -> List[str]:
        """
        Return IDs of available extractors.
        """
        return sorted(list(self._producers.keys()))

    def _create_producer_task(self, camera_id: str) -> None:
        """
        On calling, items yielded by this camera start to be published to self._stream.
        """
        task: Task = asyncio.create_task(self._stream_from_producer(camera_id))
        self._producer_tasks[camera_id] = task

    async def _stream_from_producer(self, camera_id: str) -> None:
        """
        To be called by _create_producer_task -- use it for proper scheduling instead.
        """
        if camera_id not in self._producers:
            raise ValueError(f"Can't connect producer {camera_id} to stream: ID not registered.")
        if camera_id in self._producer_tasks:
            raise ValueError(f"Producer {camera_id} already connected to stream.")
        for item in self._producers[camera_id]: # type: FrameItemRaw
            await self._stream.publish(item)

    async def __aiter__(self):
        async for item in self._stream: # type: FrameItemRaw
            yield item