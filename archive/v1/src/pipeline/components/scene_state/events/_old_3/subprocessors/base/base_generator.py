from typing import NamedTuple, Type, override
from abc import ABC, abstractmethod
from datetime import datetime

from tram_analytics.v1.models.components.scene_state.events.base import BaseSceneEventsContainer
from tram_analytics.v1.pipeline.components.scene_state.events.subprocessors.base.base_intermediary_container import BaseIntermediaryEventsContainer


class ProcessingEndedException(Exception):
    pass

class ProcessingSystemState(NamedTuple):
    # The current frame's timestamp.
    cur_frame_ts: datetime
    # Whether this frame is the first one observed
    # (signals the processor to set the `truncated` flag to `True`
    # for events produced as a result of the current call).
    is_first_frame: bool
    # NOTE: The corresponding flag is NOT being defined for the end of processing:
    # `end_processing_and_get_events`, without arguments, is to be called in this case,
    # processors likewise being expected to set the events' `truncated` flag to `True`.


class BaseEventGenerator[InputObject, EventContainer](ABC):

    def __init__(self) -> None:
        # The timestamp for the previous frame. Used to set the event timestamp
        # for events emitted by `end_processing_and_get_events`.
        self._prev_frame_ts: datetime | None = None

        # Whether this generator ended processing.
        # Set to `True` in `end_processing_and_get_events`.
        # After set to `True`, calls to `update_and_get_events` will return a `ProcessingEndedException`.
        self._ended_processing: bool = False

    @classmethod
    @abstractmethod
    def _get_empty_events_container(cls) -> EventContainer:
        pass

    @abstractmethod
    def _get_events_for_end_of_processing(self,
                                          *, event_ts: datetime,
                                          truncate_events: bool) -> EventContainer:
        pass

    @abstractmethod
    def _clear_own_state(self) -> None:
        """
        Reset any properties persisted as the instance state that were introduced by the subclass.
        Used when ending the processing.
        """
        # TODO: a better name for the method?
        pass

    def _clear_state(self) -> None:
        self._prev_frame_ts = None
        self._clear_own_state()

    # end periods before ...: use cur frame ts, do not truncate events
    # end of global processing: use prev frame ts, truncate events

    def end_processing_and_get_events_with_true_period_end(self, event_ts: datetime) -> EventContainer:
        """
        Generate end events for this period in the scenario when the period's end
        is associated with an actual observation rather than with global processing being ended.

        Example: for a processor that should exist only during the vehicle's lifetime --
        when the vehicle is no longer being tracked (as opposed to the stream being down).

        The generated events will accordingly have `truncated` set to `False`.

        :param event_ts:
          The timestamp to be associated with the events, i. e. to be used as the period's end
          (normally, this should be the current frame's timestamp).
        :return:
          A container with the emitted events.
        """
        return self._end_processing_and_get_events(event_ts=event_ts,
                                                   truncate_events=False)

    def end_processing_and_get_events_with_truncation(self) -> EventContainer:
        """
        Generate end events for this period in the scenario when the global processing is being ended
        (the stream is down, a critical runtime error has been raised, etc.)

        The **previous** frame's timestamp will be associated with the events, i. e. used as the period's end.

        The generated events will have their `truncated` attribute set to `True`
        to signal that the recorded period end might not be its true end
        because observation halted before that.
        """
        events: EventContainer = (
            self._end_processing_and_get_events(event_ts=self._prev_frame_ts,
                                                truncate_events=True)
            if self._prev_frame_ts is not None
            # if the previous frame timestamp has not been set:
            # -- can only be true if `update_and_get_events` has not been called at all.
            # Return an empty event container in that case.
            else self._get_empty_events_container()
        )
        return events

    def _end_processing_and_get_events(self, *, event_ts: datetime, truncate_events: bool) -> EventContainer:
        """
        Must be called after processing the last frame in a stream
        to produce end events for alive vehicles
        (if there are any that are moving or stationary,
        not doing so will result in an unended period).

        :param truncate_events: Whether to set `truncate` to `True` for events to be emitted.
          Set to `True` if the call is made to gracefully shut down global processing
          (for events to signal that the underlying periods might be truncated rather than full),
          or to `False` if global processing carries on and just the last events
          are being requested from this generator before it is destroyed
          (e. g. due to a vehicle's lifetime having ended and the generator no longer being needed).
        :returns: The event container.
        """
        if self._ended_processing:
            raise ProcessingEndedException()
        events: EventContainer = self._get_events_for_end_of_processing(event_ts=event_ts,
                                                                        truncate_events=truncate_events)
        # clear the updater's state
        self._clear_state()
        # set the flag to prevent further processing
        self._ended_processing = True
        return events

    @abstractmethod
    def _update_and_get_events(self, input_obj: InputObject, *, system_state: ProcessingSystemState) -> EventContainer:
        pass

    def update_and_get_events(self, input_obj: InputObject, *, system_state: ProcessingSystemState) -> EventContainer:
        """
        Update this generator's state and emit the calculated events.

        To be used during normal processing. In order to get final events before destroying the generator,
        use `end_processing_and_get_events_with_true_period_end` or `end_processing_and_get_events_with_truncation`
        instead.
        """
        if self._ended_processing:
            raise ProcessingEndedException()
        events: EventContainer = self._update_and_get_events(input_obj, system_state=system_state)

        self._prev_frame_ts = system_state.cur_frame_ts

        return events


class BaseIntermediaryEventGenerator[
    InputObject, EventContainer: BaseIntermediaryEventsContainer
](BaseEventGenerator[InputObject, EventContainer]):

    """
    A generator used to generate events wrapped in intermediary containers,
    for the later assembly of final containers from them
    (instances of `BaseEventsContainer`, which is a Pydantic `BaseModel`).
    """

    @classmethod
    @abstractmethod
    def _get_container_class(cls) -> Type[EventContainer]:
        """
        Return the concrete class of the event container
        (used to access its methods and properties in the base class).
        """
        pass

    @override
    @classmethod
    def _get_empty_events_container(cls) -> EventContainer:
        container_class: Type[EventContainer] = cls._get_container_class()
        return container_class.create_empty_container()


class BaseFinalEventGenerator[
    InputObject, EventContainer: BaseSceneEventsContainer
](BaseEventGenerator[InputObject, EventContainer]):

    """
    A generator used to generate events wrapped in final containers
    (instances of `BaseEventsContainer`, which is a Pydantic `BaseModel`).
    """

    @classmethod
    @abstractmethod
    def _get_container_class(cls) -> Type[EventContainer]:
        """
        Return the concrete class of the event container
        (used to access its methods and properties in the base class).
        """
        pass

    @override
    @classmethod
    def _get_empty_events_container(cls) -> EventContainer:
        container_class: Type[EventContainer] = cls._get_container_class()
        return container_class.create_empty_container()

class BaseGeneratorState(ABC):

    @abstractmethod
    def clear(self) -> None:
        pass
