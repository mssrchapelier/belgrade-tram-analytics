from typing import List
from asyncio import Queue

from archive.v_a1.events import ImageProcessedEvent

class ImageProcessedEventStream:
    """
    Dummy event queue for ImageProcessed events.
    Async iterate an instance to subscribe to it.
    Adapted from: https://stackoverflow.com/a/78203557
    """

    def __init__(self):
        self._subscribers: List[Queue[ImageProcessedEvent | None]] = []

    async def publish(self, e: ImageProcessedEvent | None) -> None:
        for queue in self._subscribers: # type: Queue[ImageProcessedEvent | None]
            await queue.put(e)

    async def __aiter__(self) -> ImageProcessedEvent | None:
        queue: Queue[ImageProcessedEvent | None] = Queue()
        self._subscribers.append(queue)
        try:
            while True:
                value: ImageProcessedEvent | None = await queue.get()
                yield value
                if value is None:
                    raise GeneratorExit
        except GeneratorExit:
            self._subscribers.remove(queue)
