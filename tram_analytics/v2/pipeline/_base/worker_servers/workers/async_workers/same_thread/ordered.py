from asyncio import Lock
from typing import override

from tram_analytics.v2.pipeline._base.worker_servers.workers.async_workers.base_async_workers import \
    BaseOrderedAsyncWorker
from tram_analytics.v2.pipeline._base.worker_servers.workers.base_sync_workers import BaseOrderedSyncWorker


class AsyncSameThreadOrderedWorker[InputT, OutputT](
    BaseOrderedAsyncWorker[InputT, OutputT]
):

    def __init__(self, sync_worker: BaseOrderedSyncWorker[InputT, OutputT]) -> None:
        super().__init__()

        self._sync_worker: BaseOrderedSyncWorker[InputT, OutputT] = sync_worker
        # since the worker is ordered -> stateful, employing a lock to ensure that the access to it is sequential
        # NOTE: it is the caller that is responsible for the ORDER of access
        self._lock: Lock = Lock()

    @override
    async def start(self) -> None:
        pass

    @override
    async def shutdown(self) -> None:
        pass

    @override
    async def process(self, item: InputT) -> OutputT:
        async with self._lock:
            return self._sync_worker.process(item)

    @override
    async def process_for_session_end(self) -> OutputT | None:
        async with self._lock:
            return self._sync_worker.process_for_session_end()
