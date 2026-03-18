from abc import ABC, abstractmethod


class BaseWorkerServerReadRepo[InputT](ABC):

    """
    A port for retrieving inputs.
    """

    @abstractmethod
    async def retrieve(self, frame_id: str, *, timeout: float | None) -> InputT:
        """
        Given a unique frame ID, retrieve and return the input item.
        """
        pass
