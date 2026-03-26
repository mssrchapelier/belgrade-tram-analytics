from abc import ABC
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Set, Deque, Iterator

from shapely import Polygon, LineString
from shapely.geometry.base import BaseGeometry

from tram_analytics.v1.models.common_types import BoundingBox
from tram_analytics.v1.models.components.tracking import TrackState
from tram_analytics.v1.models.components.vehicle_info import BaseRefPoints, CarRefPoints, TramRefPoints, Speeds

# --- helper functions ---

def _bbox_to_polygon(bbox: BoundingBox) -> Polygon:
    return Polygon([(bbox.x1, bbox.y1),
                    (bbox.x2, bbox.y1),
                    (bbox.x2, bbox.y2),
                    (bbox.x1, bbox.y2)])

# --- zone objects ---

# --- (1) base dataclasses ---

@dataclass(frozen=True, slots=True, kw_only=True)
class BaseGeometryContainer[G: BaseGeometry]:
    image: G
    world: G | None


@dataclass(frozen=True, slots=True, kw_only=True)
class PolygonContainer(BaseGeometryContainer[Polygon]):
    pass


@dataclass(frozen=True, slots=True, kw_only=True)
class PolylineContainer(BaseGeometryContainer[LineString]):
    pass


@dataclass(frozen=True, slots=True, kw_only=True)
class BaseZone:
    pass


@dataclass(frozen=True, slots=True, kw_only=True)
class BasePolygonZone(BaseZone):
    # The polygon defining the zone, in image (pixel) coordinates.
    polygon: PolygonContainer


@dataclass(frozen=True, slots=True, kw_only=True)
class BasePolylineZone(BaseZone):
    # The polyline defining the zone, in image (pixel) coordinates.
    polyline: PolylineContainer

# --- (2) child zone objects ---

@dataclass(frozen=True, slots=True, kw_only=True)
class IntrusionZone(BasePolygonZone):
    pass


@dataclass(frozen=True, slots=True, kw_only=True)
class RailTrack(BasePolygonZone):
    # The polyline defining the track's centreline.
    centreline: PolylineContainer


@dataclass(frozen=True, slots=True, kw_only=True)
class RailPlatform(BasePolylineZone):
    pass

# --- (3) containers ---

@dataclass(frozen=True, slots=True, kw_only=True)
class BaseZonesContainer:
    pass


@dataclass(frozen=True, slots=True, kw_only=True)
class CarZonesContainer(BaseZonesContainer):
    # intrusion zone id -> zone object
    intrusion_zones: Dict[str, IntrusionZone]


@dataclass(frozen=True, slots=True, kw_only=True)
class TramZonesContainer(BaseZonesContainer):
    # track id -> zone object
    tracks: Dict[str, RailTrack]
    # platform id -> zone object
    platforms: Dict[str, RailPlatform]
    # track id -> { platform ids }
    track_to_platforms: Dict[str, Set[str]]

# --- helper containers ---

# (1) without speed info

@dataclass(frozen=True, slots=True, kw_only=True)
class BaseInfoForSpeedCalculation[RefPoints: BaseRefPoints](ABC):
    frame_ts: datetime
    state: TrackState
    zone_ids: Set[str]
    reference_points: RefPoints


@dataclass(frozen=True, slots=True, kw_only=True)
class CarInfoForSpeedCalculation(BaseInfoForSpeedCalculation[CarRefPoints]):
    pass


@dataclass(frozen=True, slots=True, kw_only=True)
class TramInfoForSpeedCalculation(BaseInfoForSpeedCalculation[TramRefPoints]):
    pass

# (2) with speed info

@dataclass(frozen=True, slots=True, kw_only=True)
class BaseVehicleHistoryItem[RefPoints: BaseRefPoints]:
    frame_ts: datetime
    state: TrackState
    zone_ids: Set[str]
    reference_points: RefPoints
    speeds: Speeds


@dataclass(frozen=True, slots=True, kw_only=True)
class CarHistoryItem(BaseVehicleHistoryItem[CarRefPoints]):
    pass


@dataclass(frozen=True, slots=True, kw_only=True)
class TramHistoryItem(BaseVehicleHistoryItem[TramRefPoints]):
    pass


class BaseVehicleHistory[RefPoints: BaseRefPoints](ABC):

    def __init__(self, *, maxlen: int | None):
        self._items: Deque[BaseVehicleHistoryItem[RefPoints]] = deque(maxlen=maxlen)

    def add(self, item: BaseVehicleHistoryItem[RefPoints]) -> None:
        self._items.append(item)

    def get_last_item(self) -> BaseVehicleHistoryItem[RefPoints] | None:
        return self._items[-1] if len(self._items) > 0 else None

    def get_nth_item(self, idx: int) -> BaseVehicleHistoryItem[RefPoints]:
        return self._items[idx]

    def __len__(self) -> int:
        return len(self._items)

    def __iter__(self) -> Iterator[BaseVehicleHistoryItem[RefPoints]]:
        for item in self._items: # type: BaseVehicleHistoryItem[RefPoints]
            yield item


class CarHistory(BaseVehicleHistory[CarRefPoints]):
    pass


class TramHistory(BaseVehicleHistory[TramRefPoints]):
    pass
