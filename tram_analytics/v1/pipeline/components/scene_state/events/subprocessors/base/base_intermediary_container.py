from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Self, Sequence


@dataclass(slots=True, kw_only=True)
class BaseIntermediaryEventsContainer(ABC):

    """
    A mutable container for events that is meant for intermediary use
    whilst constructing a final container (a `BaseEventsContainer`).
    Implemented separately instead of as a Pydantic `BaseModel`, to reduce the associated overhead.
    """

    @classmethod
    @abstractmethod
    def create_empty_container(cls) -> Self:
        """
        Create an empty container of this type (i. e. with no events).
        """
        pass

    @classmethod
    @abstractmethod
    def concatenate(cls, containers: Sequence[Self]) -> Self:
        pass
