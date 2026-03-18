from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import Sequence, Self, TypeAlias, List, override

from pydantic import BaseModel, model_validator

from common.utils.random.id_gen import get_uuid


class BaseEvent(BaseModel):
    """
    Represents any discrete-time event.
    """

    # The camera ID for this event.
    # In principle, represents the *scene* associated with this event,
    # so can be swapped, more generally, for a "scene ID" in a multi-camera environment
    # (or applied to other spatial scenarios in general, not necessarily those
    # where computer vision is being employed), without any changes to this event hierarchy.
    camera_id: str
    # The unique ID for this event.
    event_id: str
    # The timestamp associated with this event.
    event_ts: datetime
    # A flag describing whether the timestamp corresponds to either the beginning
    # or the end of the observation period (started/stopped processing).
    # This signals any downstream processors to treat such events in a special way if needed
    # (for example, that the computed duration of the vehicle's presence inside a specific zone
    # is truncated because processing started/stopped during it).
    truncated: bool

def generate_event_uuid() -> str:
    return get_uuid()

class EventBoundaryType(str, Enum):
    START = "start"
    END = "end"

class BasePeriodBoundaryEvent(BaseEvent):
    """
    Represents any vehicle event denoting either the start or the end
    of a specific continuous period of time (e. g. the vehicle's lifetime,
    the vehicle's occupance of a specific zone, etc.)
    """
    # Whether this event represents the start or the end of the associated period.
    boundary_type: EventBoundaryType
    # The unique ID of the associated period.
    period_id: str

def generate_period_uuid() -> str:
    return get_uuid()

class BaseStatusEvent(BaseEvent):
    """
    Represents any event that is not bound to any period (as opposed to `BasePeriodBoundaryEvent`).
    Examples: speed updates, motion status updates.
    """
    pass

class BasePeriodBoundaryVehicleEvent(BasePeriodBoundaryEvent):
    vehicle_id: str

class BaseStatusVehicleEvent(BaseStatusEvent):
    vehicle_id: str

# Represents any event associated with a specific vehicle.
VehicleEvent: TypeAlias = BasePeriodBoundaryVehicleEvent | BaseStatusVehicleEvent

def have_unique_vehicle_ids(events: Sequence[VehicleEvent]) -> bool:
    """
    Check whether all vehicle IDs in `events` are unique.
    """
    num_events: int = len(events)
    num_vehicle_ids: int = len(set(e.vehicle_id for e in events))
    return num_vehicle_ids == num_events

class BaseSceneEventsContainer(BaseModel, ABC):
    """
    A base container for a group of events that are related in some way.
    """

    @classmethod
    @abstractmethod
    def create_empty_container(cls) -> Self:
        """
        Create an empty container of this type (i. e. with no events).
        """
        pass


class BaseStatusVehicleEventsContainer[Event: BaseStatusVehicleEvent](BaseSceneEventsContainer):

    updates: List[Event]

    @override
    @classmethod
    def create_empty_container(cls) -> Self:
        return cls(updates=[])

    def _check_unique_vehicle_ids(self) -> None:
        if not have_unique_vehicle_ids(self.updates):
            raise ValueError("Events cannot have duplicate vehicle IDs")

    @model_validator(mode="after")
    def _validate_model(self) -> Self:
        self._check_unique_vehicle_ids()
        return self
