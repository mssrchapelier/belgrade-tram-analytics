from aio_pika.abc import AbstractIncomingMessage, AbstractQueueIterator

async def get_next_from_aio_pika_queue_iterator(queue_iter: AbstractQueueIterator) -> AbstractIncomingMessage:
    # for use when creating tasks, to be able to pass a callable to `asyncio.create_task()`
    return await anext(queue_iter)