from typing import List

from pydantic import BaseModel
from shapely import Polygon, LineString

from common.utils.custom_types import PlanarPosition

# --- Pydantic models ---

class RailCorridorConfig(BaseModel):
    corridor_id: int
    polygon: List[PlanarPosition]
    centerline: List[PlanarPosition]

class SceneGeometryConfig(BaseModel):
    rail_corridors: List[RailCorridorConfig]

# --- classes with arbitrary type fields ---

class RailCorridor:

    def __init__(self, config: RailCorridorConfig):
        self._corridor_id: int = config.corridor_id
        self.polygon: Polygon = Polygon(config.polygon)
        self.centerline: LineString = LineString(config.centerline)