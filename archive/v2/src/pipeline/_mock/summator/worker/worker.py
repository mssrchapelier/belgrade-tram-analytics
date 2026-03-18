from typing import override

from tram_analytics.v2.pipeline._base.worker_servers.workers.base_sync_workers import BaseOrderedSyncWorker
from tram_analytics.v2.pipeline._base.worker_servers.workers.async_workers.base_async_workers import BaseOrderedAsyncWorker

from tram_analytics.v2.pipeline._mock.common.dto.data_models import Square, SumSquares
from archive.v2.src.pipeline._mock.summator.summator import Summator

class SummatorSyncWorker(BaseOrderedSyncWorker[Square, SumSquares]):

    def __init__(self, processor: Summator) -> None:
        self._processor: Summator = processor

    @override
    def start(self) -> None:
        pass

    @override
    def shutdown(self) -> None:
        pass

    @override
    def process(self, item: Square) -> SumSquares:
        return self._processor.process(item)

    @override
    def process_for_session_end(self) -> SumSquares | None:
        return self._processor.process_for_session_end()

# async wrapper: running in the same thread
class SummatorSameThreadAsyncWorker(BaseOrderedAsyncWorker[Square, SumSquares]):

    def __init__(self, sync_worker: BaseOrderedSyncWorker[Square, SumSquares]) -> None:
        self._sync_worker: BaseOrderedSyncWorker[Square, SumSquares] = sync_worker

    @override
    async def start(self) -> None:
        pass

    @override
    async def shutdown(self) -> None:
        pass

    @override
    async def process(self, item: Square) -> SumSquares:
        return self._sync_worker.process(item)

    @override
    async def process_for_session_end(self) -> SumSquares | None:
        return self._sync_worker.process_for_session_end()