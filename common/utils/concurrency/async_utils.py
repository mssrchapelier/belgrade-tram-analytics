from typing import List, AsyncIterable, AsyncIterator, Tuple
from asyncio import Queue

async def async_queue_to_list[T](q: Queue[T | None]) -> List[T]:
    """
    Consume items from an async queue and populate a list from it, in the order of ingestion,
    until a `None` sentinel has been consumed.

    Meant to be run as a task.
    """
    item_list: List[T] = []
    while True:
        item: T | None = await q.get()
        if item is None:
            break
        item_list.append(item)
    return item_list


async def aenumerate[T](iterable: AsyncIterable[T], start: int = 0) -> AsyncIterator[Tuple[int, T]]:
    """
    The equivalent of `enumerate` for use with `AsyncIterable`.
    """
    idx: int = start
    async for item in iterable: # type: T
        yield idx, item
        idx += 1
