from typing import TypeAlias

from tram_analytics.v2.pipeline._base.worker_servers.workers.async_workers.thread_executor.unordered import (
    AsyncThreadExecutorUnorderedWorker
)
from tram_analytics.v2.pipeline._mock.common.dto.data_models import EmittedNumber, Square

# async wrapper: running in a separate thread
# (always the same one, i. e. sequential access from the event loop)
SquarerAsyncThreadExecutorWorker: TypeAlias = AsyncThreadExecutorUnorderedWorker[EmittedNumber, Square]
