from abc import ABC, abstractmethod
from datetime import datetime
from typing import NamedTuple, Type, override, Final

from tram_analytics.v1.models.components.scene_state.events.base import BaseSceneEventsContainer
from tram_analytics.v1.pipeline.components.scene_state.events.subprocessors.base.base_intermediary_container import \
    BaseIntermediaryEventsContainer


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


class BaseGeneratorState(ABC):

    def __init__(self) -> None:
        # The timestamp for the previous frame. Used to set the event timestamp
        # for events emitted by `end_processing_and_get_events`.
        self.prev_frame_ts: datetime | None = None

    @abstractmethod
    def _clear_own_state(self) -> None:
        """
        Reset any properties persisted as the instance state that were introduced by the subclass.
        Used when ending the processing of the event generator that used it.
        """
        pass

    def clear(self) -> None:
        self.prev_frame_ts = None
        self._clear_own_state()


class BaseEventGenerator[InputObject, EventContainer, State: BaseGeneratorState](ABC):

    """
    A stateful abstract generator that produces events of a specific type,
    **sequentially** for **every** frame coming from a **single** camera
    (or, more generally, a single *scene*).

    At each frame, when `.update_and_get_events()` is called, it takes as input:
      (1) `InputObject` (containing whatever input data it needs, related to this frame), and
      (2) `ProcessingSystemState` (containing information such as the frame timestamp
      and whether this is the first frame in the sequence),

    and produces an instance of `EventContainer` with all events calculated from this input.

    The generator's state is maintained as a `State` instance. At the end of processing
    for this generator, one of the following two methods should be called
    to produce end events based on the generator's current state:
      - `.end_processing_and_get_events_with_true_period_end(event_ts)` if the global processing carries on.
        This signifies that the actual timestamp for the end events is known and has been provided,
        so the associated period is not truncated (the event's `truncated` flag will be set to `True`).
        Example: a processor for a single vehicle that needs to be destroyed upon the end of its lifetime
        (but with global processing continuing).
      - `.end_processing_and_get_events_with_truncation()` if the global processing has stopped.
        This signifies that the actual timestamp for the periods' end is not known
        because the global observation has stopped (due to a critical error, the pipeline's shutdown,
        or for other reasons). In this case, the stored timestamp for the **previous** frame
        will be used as the events' timestamp, and the event's `truncated` flag will be set to `True`.
    """

    def __init__(self, camera_id: str) -> None:
        self._state: Final[State] = self._get_new_state()

        # The ID of the camera (scene), inputs for which are to be processed by this generator.
        # Meant to be immutable. Stored for convenient injection into every event to be produced.
        self._camera_id: Final[str] = camera_id

        # Whether the generator ended processing.
        # Set to `True` in `end_processing_and_get_events`.
        # After set to `True`, calls to `update_and_get_events` will return a `ProcessingEndedException`.
        self._ended_processing: bool = False

    @abstractmethod
    def _get_new_state(self) -> State:
        pass

    @classmethod
    @abstractmethod
    def _get_empty_events_container(cls) -> EventContainer:
        pass

    @abstractmethod
    def _get_events_for_end_of_processing(self,
                                          *, event_ts: datetime,
                                          truncate_events: bool) -> EventContainer:
        pass

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
            self._end_processing_and_get_events(event_ts=self._state.prev_frame_ts,
                                                truncate_events=True)
            if self._state.prev_frame_ts is not None
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
        self._state.clear()
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

        self._state.prev_frame_ts = system_state.cur_frame_ts

        return events

class BaseIntermediaryEventGenerator[
    InputObject, EventContainer: BaseIntermediaryEventsContainer, State: BaseGeneratorState
](BaseEventGenerator[InputObject, EventContainer, State]):

    """
    A generator used to generate events wrapped in intermediary containers,
    for the later assembly of final containers from them
    (instances of `BaseEventsContainer`, which is a Pydantic `BaseModel`).

    Concrete implementations are meant to be auxiliary generators
    employed by instances of `BaseFinalEventGenerator` where further encapsulation
    of generation logic is beneficial (e. g., one such intermediary generator for every specific vehicle).
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
    InputObject, EventContainer: BaseSceneEventsContainer, State: BaseGeneratorState
](BaseEventGenerator[InputObject, EventContainer, State]):

    """
    A generator used to generate events wrapped in final containers
    (instances of `BaseEventsContainer`, which is a Pydantic `BaseModel`).

    Concrete implementations of this class are meant to be
    the building blocks for the event pipeline.
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