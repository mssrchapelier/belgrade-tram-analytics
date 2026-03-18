import logging
from abc import ABC, abstractmethod
from logging import Logger

from common.utils.logging_utils.logging_utils import get_logger_name_for_object


class BaseSyncWorker[InputT, OutputT](ABC):

    def __init__(self) -> None:
        self._logger: Logger = logging.getLogger(get_logger_name_for_object(self))

    @abstractmethod
    def start(self) -> None:
        pass

    @abstractmethod
    def shutdown(self) -> None:
        pass

    @abstractmethod
    def process(self, item: InputT) -> OutputT:
        pass

class BaseUnorderedSyncWorker[InputT, OutputT](
    BaseSyncWorker[InputT, OutputT], ABC
):
    pass

class BaseOrderedSyncWorker[InputT, OutputT](
    BaseSyncWorker[InputT, OutputT], ABC
):

    @abstractmethod
    def process_for_session_end(self) -> OutputT | None:
        """
        Signal to the worker that the current session has ended and get any outputs for the session's end.
        If the worker does not implement this behaviour or there is nothing to return, it should return None.

        Currently required only for the scene state processor
        (most importantly for the produced end events for the scene).
        """
        pass