import asyncio
import logging
from asyncio import AbstractEventLoop
from concurrent.futures import ThreadPoolExecutor
from logging import Logger
from typing import override

from tram_analytics.v2.pipeline._base.worker_servers.workers.async_workers.base_async_workers import \
    BaseUnorderedAsyncWorker
from tram_analytics.v2.pipeline._base.worker_servers.workers.base_sync_workers import BaseUnorderedSyncWorker


class AsyncThreadExecutorUnorderedWorker[InputT, OutputT](
    BaseUnorderedAsyncWorker[InputT, OutputT]
):

    """
    An async wrapper that runs calls to the wrapped synchronous worker's `.process()`
    in the provided `ThreadPoolExecutor`.
    """

    def __init__(self, *, sync_worker: BaseUnorderedSyncWorker[InputT, OutputT],
                 executor: ThreadPoolExecutor) -> None:
        super().__init__()
        self._sync_worker: BaseUnorderedSyncWorker[InputT, OutputT] = sync_worker
        self._executor: ThreadPoolExecutor = executor

        self._logger: Logger = logging.getLogger(__name__)

    @override
    async def start(self) -> None:
        pass

    @override
    async def shutdown(self) -> None:
        pass

    @override
    async def process(self, item: InputT) -> OutputT:
        loop: AbstractEventLoop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor,
                                          self._sync_worker.process,
                                          item)
