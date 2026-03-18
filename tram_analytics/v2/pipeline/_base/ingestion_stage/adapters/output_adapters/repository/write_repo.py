from abc import ABC, abstractmethod

from tram_analytics.v2.pipeline._base.worker_servers.adapters.output_adapters.repository.write_repo import \
    BaseWriteRepo


class BaseIngestionStageWriteRepo[OutputT](BaseWriteRepo[OutputT], ABC):

    @abstractmethod
    async def get_new_session_id(self) -> int:
        """
        Assign a new session ID for this camera (`None` if none stored).
        Is intended to monotonically increase.
        """
        pass