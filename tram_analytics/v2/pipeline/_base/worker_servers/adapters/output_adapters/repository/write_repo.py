from abc import ABC, abstractmethod


class BaseWriteRepo[OutputT](ABC):
    """
    A port for persisting outputs.
    """

    # NOTE re implementation in subclasses:
    #
    # For events and live states, two consecutive outputs with the same frame ID are possible
    # because of the necessity of emitting end events for the last seen frame when the delay threshold has been exceeded.
    # For events: the database must be updated to contain both OLD and NEW events for that frame.
    # For live state: the live state must be updated to REPLACE the old one with the new one.

    @abstractmethod
    async def store(self, *, output: OutputT, timeout: float | None) -> None:
        """
        Store the output.
        """
        # NOTE: the output type must be defined so that a unique key
        # under which to store it can be derived from it
        pass
