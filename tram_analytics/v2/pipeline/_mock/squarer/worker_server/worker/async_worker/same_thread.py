from typing import TypeAlias

from tram_analytics.v2.pipeline._base.worker_servers.workers.async_workers.same_thread.unordered import \
    AsyncSameThreadUnorderedWorker
from tram_analytics.v2.pipeline._mock.common.dto.data_models import EmittedNumber, Square

# async wrapper: running in the same thread

SquarerAsyncSameThreadWorker: TypeAlias = AsyncSameThreadUnorderedWorker[EmittedNumber, Square]