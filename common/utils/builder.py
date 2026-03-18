from abc import ABC, abstractmethod

class PropertyAlreadySetException(Exception):
    pass

class PropertyNotSetException(Exception):
    pass

class BaseBuilder[T](ABC):

    """
    A base class to implement the builder pattern for an arbitrary type `T`.
    """

    @abstractmethod
    def build(self) -> T:
        pass