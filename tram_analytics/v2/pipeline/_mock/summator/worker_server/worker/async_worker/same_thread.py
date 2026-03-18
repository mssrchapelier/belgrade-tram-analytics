from typing import TypeAlias

from tram_analytics.v2.pipeline._base.worker_servers.workers.async_workers.same_thread.ordered import \
    AsyncSameThreadOrderedWorker
from tram_analytics.v2.pipeline._mock.common.dto.data_models import Square, SumSquares

# async wrapper: running in the same thread

SummatorAsyncSameThreadWorker: TypeAlias = AsyncSameThreadOrderedWorker[Square, SumSquares]