from typing import List, Dict, Set

import shapely
from shapely import Polygon, Point
from shapely.geometry.base import BaseGeometry
from pydantic import BaseModel

from tram_analytics.v1.models.common_types import BoundingBox
from archive.v1.src.v_0_2_0.pipeline.components.analytics.scene_geometry.scene_geometry import RailCorridor, RailCorridorConfig
from common.utils.shapely_utils import bbox_to_polygon
from common.utils.custom_types import PlanarPosition

class TramPositionalProxies(BaseModel):
    # The projections onto the rail corridor's centreline
    # of three points of the intersection of the tram's bounding box
    # with the rail corridor polygon:

    # 1. the forwardmost (w.r.t. the centreline) point
    proj_span_start: PlanarPosition
    # 2. the centroid
    proj_span_center: PlanarPosition
    # 3. the backwardmost point
    proj_span_end: PlanarPosition

def determine_rail_corridor(tram_bbox: BoundingBox, corridors: Dict[int, RailCorridor]) -> Set[int]:
    """
    Return the IDs of the rail corridors with the largest polygon area inside the tram's bounding box.
    Used to determine on which track the tram is located.

    Note: The returned set is empty if all intersections have an area of 0
    (i. e. if the bounding box does not intersect any of the corridors).

    :argument tram_bbox: the tram's bounding box (floating-point values)
    :argument corridors: a `Dict` mapping the corridors' IDs to the respective instances of `RailCorridor`
    :returns The set of IDs of the rail corridor with the largest polygon area inside the tram's bounding box.
    """
    bbox_polygon: Polygon = bbox_to_polygon(tram_bbox)

    if not isinstance(bbox_polygon, Polygon):
        raise ValueError(f"tram_bbox is not a valid polygon: {str(tram_bbox.to_xyxy_list())}")

    max_intersection_area: float = 0.0
    ids_with_max_area: Set[int] = set()
    for corridor_id, corridor in corridors.items(): # type: int, RailCorridor
        if not isinstance(corridor.polygon, Polygon):
            raise ValueError(f"Invalid polygon in corridor: {str(list(corridor.polygon.coords))}")
        intersection_area: float = shapely.intersection(bbox_polygon, corridor.polygon).area
        if intersection_area >= max_intersection_area:
            if intersection_area > max_intersection_area:
                ids_with_max_area.clear()
            if intersection_area > 0.0:
                ids_with_max_area.add(corridor_id)
            max_intersection_area = intersection_area

    if len(ids_with_max_area) > 1:
        print(f"WARNING: More than one max area of intersection. Max area: {max_intersection_area:.6f}")

    return ids_with_max_area

def get_tram_positional_proxies(tram_bbox: BoundingBox, corridor: RailCorridor) -> TramPositionalProxies | None:
    bbox_polygon: Polygon = bbox_to_polygon(tram_bbox)
    intersection: BaseGeometry = shapely.intersection(bbox_polygon, corridor.polygon)
    if not isinstance(intersection, Polygon) or intersection.area == 0.0:
        print("WARNING: Intersection of tram_bbox and corridor is 0. tram_bbox: {} | corridor centerline: {}".format(
            str(tram_bbox.to_xyxy_list()), str(list(corridor.centerline.coords))
        ))
        return None
    centroid: Point = intersection.centroid
    # project the centroid onto the centreline
    span_center_dist: float = corridor.centerline.project(centroid)
    span_center: Point = corridor.centerline.interpolate(span_center_dist)
    # project all vertices of the intersection onto the centerline
    proj_vertices_distances: List[float] = [
        corridor.centerline.project(Point(vertex_x, vertex_y))
        for vertex_x, vertex_y in list(intersection.exterior.coords)
    ]
    # lowest distance -> span_start
    span_start_dist: float = min(proj_vertices_distances)
    span_start: Point = corridor.centerline.interpolate(span_start_dist)
    # highest distance -> span_end
    span_end_dist: float = max(proj_vertices_distances)
    span_end: Point = corridor.centerline.interpolate(span_end_dist)

    proxies: TramPositionalProxies = TramPositionalProxies(
        proj_span_start=(span_start.x, span_start.y),
        proj_span_center=(span_center.x, span_center.y),
        proj_span_end=(span_end.x, span_end.y)
    )
    return proxies

class TramProcessorOutput(BaseModel):
    corridor_id: int | None
    proxies: TramPositionalProxies | None

class TramProcessor:

    def __init__(self, corridor_configs: List[RailCorridorConfig]):
        # corridor ID -> RailCorridor
        self._corridors: Dict[int, RailCorridor] = {config.corridor_id: RailCorridor(config)
                                                    for config in corridor_configs}

    def _get_corridor_id(self, tram_bbox: BoundingBox) -> int | None:
        """
        Determine the ID of the rail corridor to which this tram's bounding box is to be assigned.

        Notes:
             1. May return None if the box does not intersect any of the corridors
             (more exactly, if `determine_rail_corridor()` has returned an empty set).
             Prints a warning in this case.
             2. If there is more than one corridor with the maximum area of intersection,
             prints a warning and returns a RANDOM choice out of such corridors.
             TODO: Log messages properly
        """
        corridor_ids: Set[int] = determine_rail_corridor(tram_bbox, self._corridors)

        if len(corridor_ids) == 0:
            msg: str = (
                "WARNING: The bounding box was not assigned to any of the corridors."
                + "\nBounding box: {}".format(str(tram_bbox.to_xyxy_list()))
            )
            print(msg)
            return None
        if len(corridor_ids) > 1:
            all_ids: List[int] = sorted(list(corridor_ids))
            corridor_id: int = corridor_ids.pop()
            msg: str = (
                "WARNING: More than one max area of intersection of tram_bbox with corridors. "
                + "Got corridor IDs: {}. Returning just one: {}".format(", ".join(str(c_id) for c_id in all_ids),
                                                                        corridor_id)
                + "\nBounding box: {}".format(str(tram_bbox.to_xyxy_list()))
            )
            print(msg)
            return corridor_id
        return corridor_ids.pop()

    def process_tram_bbox(self, bbox: BoundingBox) -> TramProcessorOutput:
        """
        Assign the passed bounding box to AT MOST ONE of corridors and compute the proxy points.
        """
        corridor_id: int | None = self._get_corridor_id(bbox)
        proxies: TramPositionalProxies | None = (
            get_tram_positional_proxies(bbox, self._corridors[corridor_id])
            if corridor_id is not None
            else None
        )
        output: TramProcessorOutput = TramProcessorOutput(
            corridor_id=corridor_id, proxies=proxies
        )
        return output