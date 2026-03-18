import asyncio
from asyncio import Lock, AbstractEventLoop
from concurrent.futures import ThreadPoolExecutor
from typing import override

from tram_analytics.v2.pipeline._base.worker_servers.workers.async_workers.base_async_workers import \
    BaseOrderedAsyncWorker
from tram_analytics.v2.pipeline._base.worker_servers.workers.base_sync_workers import BaseOrderedSyncWorker


class AsyncThreadExecutorOrderedWorker[InputT, OutputT](
    BaseOrderedAsyncWorker[InputT, OutputT]
):

    """
    An async wrapper that runs calls to the wrapped synchronous worker's `.process()`
    in the provided `ThreadPoolExecutor`.
    """

    def __init__(self, *, sync_worker: BaseOrderedSyncWorker[InputT, OutputT],
                 executor: ThreadPoolExecutor) -> None:
        super().__init__()

        self._sync_worker: BaseOrderedSyncWorker[InputT, OutputT] = sync_worker
        self._executor: ThreadPoolExecutor = executor

        # For every instance of this class,
        # because the wrapped worker is ORDERED,
        # only one call to `.process()`, `.process_for_session_end()` will be executed at any given time.
        # This is ensured by locking the operation under asyncio lock.
        # OTHER instances of this worker (i. e. for other streams) may access the thread pool executor independently,
        # each with their own asyncio lock, so multiple streams can access the pool executor in parallel,
        # but only one operation will be performed per stream at any given time.
        self._lock: Lock = Lock()

    @override
    async def start(self) -> None:
        pass

    @override
    async def shutdown(self) -> None:
        pass

    @override
    async def process(self, item: InputT) -> OutputT:
        loop: AbstractEventLoop = asyncio.get_running_loop()
        async with self._lock:
            self._logger.debug("LOCK ACQUIRED")
            result: OutputT = await loop.run_in_executor(self._executor,
                                                         self._sync_worker.process,
                                                         item)
        self._logger.debug("LOCK RELEASED")
        return result

    @override
    async def process_for_session_end(self) -> OutputT | None:
        loop: AbstractEventLoop = asyncio.get_running_loop()
        async with self._lock:
            self._logger.debug("LOCK ACQUIRED")
            result: OutputT | None =  await loop.run_in_executor(
                self._executor,
                self._sync_worker.process_for_session_end
            )
            self._logger.debug("LOCK RELEASED")
        return result