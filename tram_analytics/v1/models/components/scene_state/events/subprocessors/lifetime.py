from typing import Literal, List, override, Self

from pydantic import model_validator

from tram_analytics.v1.models.common_types import VehicleType
from tram_analytics.v1.models.components.scene_state.events.base import BasePeriodBoundaryVehicleEvent, \
    EventBoundaryType, BaseSceneEventsContainer, \
    have_unique_vehicle_ids


# --- vehicle lifetime events ---

class BaseVehicleLifetimeEvent(BasePeriodBoundaryVehicleEvent):
    """
    Represents the start or end of a vehicle's lifetime,
    defined as a continuous period during which the vehicle exists at every moment (frame).
    """
    pass


class VehicleLifetimeStartEvent(BaseVehicleLifetimeEvent):
    """
    An event representing the start of the vehicle's lifetime (tracking has started),
    i. e. the associated frame is the first one in which it has appeared.
    """
    boundary_type: Literal[EventBoundaryType.START] = EventBoundaryType.START
    vehicle_type: VehicleType


class VehicleLifetimeEndEvent(BaseVehicleLifetimeEvent):
    """
    An event representing the end of the vehicle's lifetime (tracking has ended),
    i. e. the PREVIOUS frame wrt the associated one was the last one in which the vehicle appeared.
    Under the current approach used, this vehicle can never reappear again
    if this event has been emitted.
    """
    boundary_type: Literal[EventBoundaryType.END] = EventBoundaryType.END


# VehicleLifetimeEvent: TypeAlias = Annotated[
#     VehicleLifetimeStartEvent | VehicleLifetimeEndEvent,
#     Field(discriminator="boundary_type")
# ]

class VehiclesLifetimeEventsContainer(BaseSceneEventsContainer):

    """
    A container for all registered vehicle lifetime events for this frame.
    """

    start: List[VehicleLifetimeStartEvent]
    end: List[VehicleLifetimeEndEvent]

    @override
    @classmethod
    def create_empty_container(cls) -> Self:
        return cls(start=[], end=[])

    def _check_vehicle_ids_unique(self) -> None:
        if not have_unique_vehicle_ids((*self.start, *self.end)):
            raise ValueError("Vehicle IDs in start, end must be unique")

    @model_validator(mode="after")
    def _validate_model(self) -> Self:
        # all vehicle ids must be unique
        self._check_vehicle_ids_unique()
        return self
