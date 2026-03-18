from typing import Literal, TypeAlias, Annotated, List, Self

from pydantic import Field, BaseModel, model_validator

from tram_analytics.v1.models.components.scene_state.events.base import BasePeriodBoundaryVehicleEvent, EventBoundaryType, have_unique_vehicle_ids

class BaseStationaryEvent_Old(BasePeriodBoundaryVehicleEvent):
    pass

class StationaryStartEvent_Old(BaseStationaryEvent_Old):
    boundary_type: Literal[EventBoundaryType.START] = EventBoundaryType.START

class StationaryEndEvent_Old(BaseStationaryEvent_Old):
    boundary_type: Literal[EventBoundaryType.END] = EventBoundaryType.END

StationaryEvent_Old: TypeAlias = Annotated[
    StationaryStartEvent_Old | StationaryEndEvent_Old,
    Field(discriminator="boundary_type")
]

class StationaryEventsContainer_Old(BaseModel):
    start: List[StationaryStartEvent_Old]
    end: List[StationaryEndEvent_Old]

    def _check_vehicle_ids_unique(self) -> None:
        if not have_unique_vehicle_ids((*self.start, *self.end)):
            raise ValueError("Vehicle IDs in start, end must be unique")

    @model_validator(mode="after")
    def _validate_model(self) -> Self:
        # all vehicle ids must be unique
        self._check_vehicle_ids_unique()
        return self
