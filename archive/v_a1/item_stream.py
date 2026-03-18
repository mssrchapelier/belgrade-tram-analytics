from typing import Generic, TypeVar, List
from asyncio import Queue

class ItemStream[T]:
    """
    Event/item queue.
    Async iterate an instance to subscribe to it.
    Adapted from: https://stackoverflow.com/a/78203557
    """

    def __init__(self):
        self._subscribers: List[Queue[T | None]] = []

    async def publish(self, item: T | None) -> None:
        for queue in self._subscribers:  # type: Queue[T | None]
            await queue.put(item)

    async def __aiter__(self) -> T | None:
        queue: Queue[T | None] = Queue()
        self._subscribers.append(queue)
        try:
            while True:
                item: T | None = await queue.get()
                yield item
                if item is None:
                    raise GeneratorExit
        except GeneratorExit:
            self._subscribers.remove(queue)