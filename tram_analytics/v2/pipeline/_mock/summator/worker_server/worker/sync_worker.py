from typing import override

from tram_analytics.v2.pipeline._base.worker_servers.workers.base_sync_workers import BaseOrderedSyncWorker
from tram_analytics.v2.pipeline._mock.common.dto.data_models import Square, SumSquares
from tram_analytics.v2.pipeline._mock.summator.worker_server.worker.core.summator import Summator


class SummatorSyncWorker(BaseOrderedSyncWorker[Square, SumSquares]):

    def __init__(self, processor: Summator) -> None:
        super().__init__()
        self._processor: Summator = processor

    @override
    def start(self) -> None:
        pass

    @override
    def shutdown(self) -> None:
        pass

    @override
    def process(self, item: Square) -> SumSquares:
        result: SumSquares = self._processor.process(item)
        return result

    @override
    def process_for_session_end(self) -> SumSquares | None:
        result: SumSquares | None = self._processor.process_for_session_end()
        return result