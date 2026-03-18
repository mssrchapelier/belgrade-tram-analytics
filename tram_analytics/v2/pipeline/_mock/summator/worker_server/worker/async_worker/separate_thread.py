from typing import TypeAlias

from tram_analytics.v2.pipeline._base.worker_servers.workers.async_workers.thread_executor.ordered import \
    AsyncThreadExecutorOrderedWorker
from tram_analytics.v2.pipeline._mock.common.dto.data_models import Square, SumSquares

# async wrapper: running in a separate thread
# (always the same one, i. e. sequential access from the event loop)
SummatorAsyncThreadExecutorWorker: TypeAlias = AsyncThreadExecutorOrderedWorker[Square, SumSquares]
