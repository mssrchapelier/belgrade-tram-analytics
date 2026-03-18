from typing import Self

from pydantic import BaseModel

from tram_analytics.v1.dashboard.models.vehicles import VehiclesContainer
from tram_analytics.v1.dashboard.models.zones import ZonesContainer
from tram_analytics.v1.models.components.scene_state.live_state.live_state import (
    LiveAnalyticsState, LiveStateMetadata
)


class LiveStateForRender(BaseModel):
    """
    Changes `LiveAnalyticsState` as follows, for easier rendering with templates:
    - platforms containers in `ZonesContainer` and in each `ZoneInfosForTramsContainer`
    are moved into track containers (to render platform information inside the block
    for the track to which they belong).
    """
    api_version: str
    metadata: LiveStateMetadata
    zones: ZonesContainer
    vehicles: VehiclesContainer

    @classmethod
    def from_source(cls, src_model: LiveAnalyticsState) -> Self:
        return cls(
            api_version=src_model.api_version,
            metadata=src_model.metadata,
            zones=ZonesContainer.from_source(src_model.zones),
            vehicles=VehiclesContainer.from_source(src_model.vehicles)
        )