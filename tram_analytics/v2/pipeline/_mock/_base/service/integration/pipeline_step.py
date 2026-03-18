from abc import ABC, abstractmethod
from enum import StrEnum, auto

class WorkerThreadingOption(StrEnum):
    SAME_THREAD = auto()
    WITH_THREAD_EXECUTOR = auto()

class BasePipelineStep(ABC):

    """
    A pipeline step for a single stream.
    Wraps the pipeline stage object and, if a unique worker server is bundled with it, also the worker server
    for convenient management of the step's lifecycle.
    """

    @abstractmethod
    async def start(self) -> None:
        pass

    @abstractmethod
    async def shutdown(self) -> None:
        pass