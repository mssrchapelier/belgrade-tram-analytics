import asyncio
import logging
from asyncio import AbstractEventLoop
from concurrent.futures import ThreadPoolExecutor
from logging import Logger
from typing import override

from tram_analytics.v2.pipeline._base.worker_servers.workers.async_workers.base_async_workers import BaseUnorderedAsyncWorker
from tram_analytics.v2.pipeline._base.worker_servers.workers.base_sync_workers import BaseUnorderedSyncWorker


class AsyncSeparateThreadWorker[InputT, OutputT](
    BaseUnorderedAsyncWorker[InputT, OutputT]
):

    """
    An async wrapper that runs calls to the wrapped synchronous worker's `.process()`
    in a dedicated single thread (through a `ThreadPoolExecutor` with `max_workers` set to `1`).
    """

    def __init__(self, sync_worker: BaseUnorderedSyncWorker[InputT, OutputT]) -> None:
        self._sync_worker: BaseUnorderedSyncWorker[InputT, OutputT] = sync_worker
        self._executor: ThreadPoolExecutor | None = None

        self._logger: Logger = logging.getLogger(__name__)


    @override
    async def start(self) -> None:

        if self._executor is not None:
            self._logger.warning("Requested start but is already started")
            return
        # IMPORTANT: Just one thread because it is presupposed that the sync worker
        # cannot be safely mutated non-sequentially.
        # Alternatively, a threading lock can be used
        # (or async lock since start is supposed to be called
        # from a single worker server -> normally a single event loop at all times),
        # but a threading lock or this executor are more reliable
        # in the case the coroutine is ever called from two loops.
        self._executor = ThreadPoolExecutor(max_workers=1)


    @override
    async def shutdown(self) -> None:
        if self._executor is None:
            self._logger.warning("Requested a shutdown but has already been shut down")
            return
        self._executor.shutdown()
        self._executor = None

    @override
    async def process(self, item: InputT) -> OutputT:
        if self._executor is None:
            raise RuntimeError("Can't process item: the thread pool executor has not been started "
                               "(call .start() on this instance first)")
        loop: AbstractEventLoop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor,
                                          self._sync_worker.process,
                                          item)
