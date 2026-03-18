import logging
from abc import ABC, abstractmethod
from logging import Logger

from common.utils.logging_utils.logging_utils import get_logger_name_for_object


class BaseAsyncWorker[InputT, OutputT](ABC):

    def __init__(self) -> None:
        self._logger: Logger = logging.getLogger(get_logger_name_for_object(self))

    @abstractmethod
    async def start(self) -> None:
        pass

    @abstractmethod
    async def shutdown(self) -> None:
        pass

    @abstractmethod
    async def process(self, item: InputT) -> OutputT:
        pass

class BaseUnorderedAsyncWorker[InputT, OutputT](
    BaseAsyncWorker[InputT, OutputT], ABC
):
    pass

class BaseOrderedAsyncWorker[InputT, OutputT](
    BaseAsyncWorker[InputT, OutputT], ABC
):

    @abstractmethod
    async def process_for_session_end(self) -> OutputT | None:
        """
        Signal to the worker that the current session has ended and get any outputs for the session's end.
        If the worker does not implement this behaviour or there is nothing to return, it should return None.

        Currently required only for the scene state processor
        (most importantly for the produced end events for the scene).
        """
        pass