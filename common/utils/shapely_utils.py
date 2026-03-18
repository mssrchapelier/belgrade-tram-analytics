from typing import Tuple, List

import shapely.ops as sh_ops
from shapely import Point, LineString, Polygon
from shapely.geometry.base import BaseGeometry

from tram_analytics.v1.models.common_types import BoundingBox


def point_is_in_geometry(*, pt: Point,
                         geometry: BaseGeometry,
                         tolerance: float) -> bool:
    # compare the distance from the reference point to the platform line
    # by comparing it to the tolerance threshold
    # (to account for floating-point rounding errors)
    distance: float = geometry.distance(pt)
    return distance < tolerance

def get_linestring_segment(endpoints: Tuple[Point, Point],
                           src_linestring: LineString,
                           *, endpoint_tolerance: float) -> LineString:
    """
    Given a `LineString` and two calculated points on it that define the segment's start and end
    (all meant to be in image coordinates), return the segment lying between these two points.
    """
    # if not all(src_linestring.contains(pt) for pt in endpoints):
    if not all(
            point_is_in_geometry(pt=pt, geometry=src_linestring, tolerance=endpoint_tolerance)
            for pt in endpoints
    ):
        raise ValueError("Invalid endpoints: at least one is not on the source linestring")
    if endpoints[0].equals(endpoints[1]):
        raise ValueError("Invalid endpoints: are equal")

    start, end = endpoints  # type: Point, Point

    # (1) split the line by the start point
    splits_by_start: List[BaseGeometry] = list(sh_ops.split(src_linestring, start).geoms)
    # pick the segment that contains the end point
    segment_with_end_wrapped: List[BaseGeometry] = list(filter(
        lambda segment: point_is_in_geometry(pt=end, geometry=segment, tolerance=endpoint_tolerance),
        splits_by_start
    ))
    if not len(segment_with_end_wrapped) == 1:
        raise RuntimeError("Got invalid segments after splitting the source linestring by start point: "
                           "got not exactly one that contains the end point")
    segment_with_end: BaseGeometry = segment_with_end_wrapped[0]
    if not isinstance(segment_with_end, LineString):
        raise RuntimeError("Got invalid segment after splitting the source linestring by start point: "
                           "not a LineString")

    # (2) split the segment by the end point
    segments_down_from_start: List[BaseGeometry] = list(sh_ops.split(segment_with_end, end).geoms)
    # pick the segment whose boundary contains the start point
    # --> this is the one to return (its boundary contains both the start and the end points)
    segment_with_start_wrapped: List[BaseGeometry] = list(filter(
        lambda segment: point_is_in_geometry(pt=start, geometry=segment, tolerance=endpoint_tolerance),
        segments_down_from_start
    ))
    if not len(segment_with_start_wrapped) == 1:
        raise RuntimeError("Got invalid segment after splitting the source linestring by end point: "
                           "got more than one that contains the start point")
    segment_with_start: BaseGeometry = segment_with_start_wrapped[0]
    if not isinstance(segment_with_start, LineString):
        raise RuntimeError("Got invalid segment after splitting the source linestring by end point: "
                           "not a LineString")

    return segment_with_start

def get_point_coords(pt: Point) -> Tuple[float, ...]:
    coords: List[Tuple[float, ...]] = list(pt.coords)
    if len(coords) != 1:
        raise ValueError("Unexpected input (must be a Point): coordinates for more than one defining point")
    return coords[0]


def bbox_to_polygon(bbox: BoundingBox) -> Polygon:
    polygon: Polygon = Polygon(
        [(bbox.x1, bbox.y1), (bbox.x2, bbox.y1),
         (bbox.x2, bbox.y2), (bbox.x1, bbox.y2)]
    )
    return polygon
