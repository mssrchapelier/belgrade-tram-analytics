from typing import override

from tram_analytics.v2.pipeline._base.worker_servers.workers.base_sync_workers import BaseUnorderedSyncWorker
from tram_analytics.v2.pipeline._mock.common.dto.data_models import EmittedNumber, Square
from tram_analytics.v2.pipeline._mock.squarer.worker_server.worker.core.squarer import Squarer


class SquarerSyncWorker(BaseUnorderedSyncWorker[EmittedNumber, Square]):

    def __init__(self, processor: Squarer) -> None:
        super().__init__()
        self._processor: Squarer = processor

    @override
    def start(self) -> None:
        pass

    @override
    def shutdown(self) -> None:
        pass

    @override
    def process(self, item: EmittedNumber) -> Square:
        return self._processor.process(item)
