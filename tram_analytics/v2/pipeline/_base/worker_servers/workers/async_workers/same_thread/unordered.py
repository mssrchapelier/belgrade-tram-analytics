from typing import override

from tram_analytics.v2.pipeline._base.worker_servers.workers.async_workers.base_async_workers import \
    BaseUnorderedAsyncWorker
from tram_analytics.v2.pipeline._base.worker_servers.workers.base_sync_workers import BaseUnorderedSyncWorker


class AsyncSameThreadUnorderedWorker[InputT, OutputT](
    BaseUnorderedAsyncWorker[InputT, OutputT]
):

    def __init__(self, sync_worker: BaseUnorderedSyncWorker[InputT, OutputT]) -> None:
        super().__init__()

        self._sync_worker: BaseUnorderedSyncWorker[InputT, OutputT] = sync_worker

    @override
    async def start(self) -> None:
        pass

    @override
    async def shutdown(self) -> None:
        pass

    @override
    async def process(self, item: InputT) -> OutputT:
        return self._sync_worker.process(item)