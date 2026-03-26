from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Any, TypeGuard, Tuple, DefaultDict, Set, override

import shapely as sh
from shapely import LineString, Point, Polygon
from shapely.geometry.base import BaseGeometry

from common.utils.custom_types import PlanarPosition, is_planar_position, ensure_is_planar_position
from common.utils.shapely_utils import get_linestring_segment, get_point_coords, point_is_in_geometry
from tram_analytics.v1.models.components.tracking import TrackState
from tram_analytics.v1.models.components.vehicle_info import (
    TramRefPoints, TramInfo, PositionContainer, TrackCentrelinePositions, Speeds
)
from tram_analytics.v1.pipeline.components.vehicle_info.components.containers import (
    TramZonesContainer, RailTrack, PolygonContainer, PolylineContainer, RailPlatform,
    BaseVehicleHistory, _bbox_to_polygon, BaseInfoForSpeedCalculation, TramInfoForSpeedCalculation,
    TramHistory, TramHistoryItem, BaseVehicleHistoryItem
)
from tram_analytics.v1.pipeline.components.vehicle_info.components.coord_conversion.homography import CoordConverter
from tram_analytics.v1.pipeline.components.vehicle_info.components.speeds.config import SpeedCalculatorConfig
from tram_analytics.v1.pipeline.components.vehicle_info.components.zones.zones_config import (
    RailTrackConfig, RailPlatformConfig, BaseSingleZoneConfig, RailTrackCoordsConfig,
    RailPlatformCoordsConfig, RailPlatformEndpointSupportingLine
)
from tram_analytics.v1.pipeline.components.vehicle_info.settings import SHAPELY_POINT_ON_LINE_DISTANCE_TOLERANCE_IMAGECOORDS
from tram_analytics.v1.pipeline.components.vehicle_info.updater.base import BaseZoneAndSpeedAssigner


class TramZoneAndSpeedAssigner(BaseZoneAndSpeedAssigner[TramRefPoints, TramInfo]):

    def __init__(self,
                 *, rail_track_config: RailTrackConfig,
                 platform_config: RailPlatformConfig,
                 coord_converter: CoordConverter | None,
                 speed_config: SpeedCalculatorConfig) -> None:
        super().__init__(coord_converter=coord_converter, speed_config=speed_config)
        self._zones: TramZonesContainer = self._build_zones(rail_track_config, platform_config)

    def _track_mappings_from_config(self, config: RailTrackConfig) -> Dict[str, RailTrack]:
        tracks: Dict[str, RailTrack] = dict()
        for cfg in config.zones:  # type: BaseSingleZoneConfig[RailTrackCoordsConfig]
            polygon_container: PolygonContainer = self._build_polygon_container(cfg.coords.polygon)
            polyline_container: PolylineContainer = self._build_polyline_container(cfg.coords.centreline)
            track: RailTrack = RailTrack(polygon=polygon_container,
                                         centreline=polyline_container)
            tracks[cfg.zone_id] = track
        return tracks

    def _is_track_id(self, zone_id: str) -> bool:
        return zone_id in self._zones.tracks.keys()

    def _is_platform_id(self, zone_id: str) -> bool:
        return zone_id in self._zones.platforms.keys()

    @staticmethod
    def _find_platform_endpoint(supporting_line: LineString, track_centreline: LineString) -> Point:
        endpoint: BaseGeometry = sh.intersection(supporting_line, track_centreline)
        if not isinstance(endpoint, Point):
            raise ValueError(
                "Invalid platform geometry config: the intersection "
                "of the endpoint supporting line and the track's centreline is not a point."
            )
        return endpoint

    @staticmethod
    def _is_list_of_planar_positions(pos_list: List[Any]) -> TypeGuard[List[PlanarPosition]]:
        return all(is_planar_position(pos) for pos in pos_list)

    def _build_platform_polyline(self,
                                 supports_coords_config: RailPlatformCoordsConfig,
                                 track: RailTrack
                                 ) -> List[PlanarPosition]:
        """
        Builds a polyline (in IMAGE coordinates) for the platform and returns the defining points.
        """
        track_centreline: LineString = track.centreline.image
        line_coords_all: Tuple[RailPlatformEndpointSupportingLine, RailPlatformEndpointSupportingLine] = supports_coords_config.platform_endpoints_supporting_lines
        # build polyline shapes for the supporting lines
        supporting_lines: Tuple[LineString, LineString] = (
            LineString(line_coords_all[0]),
            LineString(line_coords_all[1])
        )
        # find the intersection of each supporting line with the track centreline
        endpoints: Tuple[Point, Point] = (
            self._find_platform_endpoint(supporting_lines[0], track_centreline),
            self._find_platform_endpoint(supporting_lines[1], track_centreline)
        )
        # build the platform shape from the calculated endpoints
        platform: LineString = get_linestring_segment(
            endpoints, track_centreline,
            endpoint_tolerance=SHAPELY_POINT_ON_LINE_DISTANCE_TOLERANCE_IMAGECOORDS
        )

        platform_coords: List[Tuple[float, ...]] = list(platform.coords)
        if not self._is_list_of_planar_positions(platform_coords):
            raise RuntimeError("Got invalid points from _build_platform_polyline() (not 2D points)")

        return platform_coords

    def _build_platform(self,
                        supports_coords_config: RailPlatformCoordsConfig,
                        track: RailTrack) -> RailPlatform:
        platform_coords_img: List[PlanarPosition] = self._build_platform_polyline(supports_coords_config, track)
        platform_polyline_container: PolylineContainer = self._build_polyline_container(platform_coords_img)
        platform: RailPlatform = RailPlatform(polyline=platform_polyline_container)
        return platform

    def _build_zones(self,
                     track_config: RailTrackConfig,
                     platform_config: RailPlatformConfig) -> TramZonesContainer:
        # tracks: just build the polygons and polylines
        # platforms:
        # (1) build the two supporting polylines for each
        # (2) determine the points at which they intersect the track's centreline
        # (3) connect these two points with a line segment following the track's centreline and store it

        # track id -> zone object
        tracks: Dict[str, RailTrack] = self._track_mappings_from_config(track_config)
        # Ensure that there is at least one track: the current implementation of this assigner
        # depends on the presence of at least one track to calculate reference points for trams
        # because these are calculated with respect to a track.
        if len(tracks) == 0:
            raise ValueError("Invalid configuration: At least one rail track must be configured "
                             "(reference point calculation for trams depends on the presence "
                             "of at least one rail track).")

        # platform id -> zone object
        platforms: Dict[str, RailPlatform] = dict()
        # track id -> { platform ids }
        track_to_platforms: DefaultDict[str, Set[str]] = defaultdict(set)
        for platform_cfg in platform_config.zones: # type: BaseSingleZoneConfig[RailPlatformCoordsConfig]
            track_id: str = platform_cfg.coords.track_zone_id
            platform_id: str = platform_cfg.zone_id

            track: RailTrack = tracks[track_id]

            platform: RailPlatform = self._build_platform(platform_cfg.coords, track)

            platforms[platform_cfg.zone_id] = platform
            track_to_platforms[track_id].add(platform_id)
        container: TramZonesContainer = TramZonesContainer(
            tracks=tracks, platforms=platforms, track_to_platforms=track_to_platforms
        )
        return container

    def _calculate_rail_track_id_and_ref_points(
            self, *, vehicle_id: str, bbox_polygon: Polygon
    ) -> Tuple[str | None, TramRefPoints]:
        # [ (track id, the area of the intersection of bbox and the track zone) ]

        # Note: Only positive intersection areas are considered
        # (it makes no sense to assign a track to a tram
        # with which the tram's bounding box does not intersect at all).

        # Note 2: Only the vehicle's bounding box is
        max_intersection_area: float | None = None
        ids_with_max_area: Set[str] = set()

        for track_id, zone in self._zones.tracks.items():  # type: str, RailTrack
            # calculate the area of intersection of bbox and track rail polygon
            intersection_area: float = sh.intersection(bbox_polygon, zone.polygon.image).area
            # update max value and id set as needed
            if max_intersection_area is None or (intersection_area > 0.0
                                                 and intersection_area >= max_intersection_area):
                if max_intersection_area is not None and intersection_area > max_intersection_area:
                    # found new max, reset the id set
                    ids_with_max_area.clear()
                max_intersection_area = intersection_area
                # add the id to set
                ids_with_max_area.add(track_id)

        assigned_track_id: str | None = self._select_track_id_from_withmaxarea(
            vehicle_id=vehicle_id, ids_with_max_area=ids_with_max_area
        )

        # select the ID of the track on the basis of which the reference points will be calculated
        refpoints_track: RailTrack | None = (
            self._zones.tracks[assigned_track_id]
            if assigned_track_id is not None
            else None
        )
        ref_points: TramRefPoints = self._calculate_ref_points(bbox_polygon, refpoints_track)

        # If the centreline reference points have been set to null, that means the vehicle's bounding box
        # does not intersect the rail track's centreline. In this case, it makes no sense to assign this vehicle
        # to the calculated track; set the track ID to null.
        if ref_points.vehicle_centreline is None:
            assigned_track_id = None

        return assigned_track_id, ref_points

    @staticmethod
    def _ensure_track_ids_is_singleton(track_ids: Set[str]) -> Set[str]:
        if len(track_ids) > 1:
            raise ValueError("track_ids contains more than one element -- disallowed in this implementation")
        return track_ids

    def _get_last_assigned_nonnull_track_id(self, vehicle_id: str) -> str | None:
        """
        Get the last non-`None` rail track ID that was assigned to this vehicle in the current history window.
        If the history is empty or if no rail track was assigned to this vehicle inside the window,
        return `None`.

        Meant to be used to determine which track ID to assign in the case of a conflict
        (see `_select_track_id_from_withmaxarea()`).
        """
        # history: Deque[BaseVehicleHistoryItem[TramRefPoints]] = self._get_history_for_vehicle(vehicle_id)
        history: BaseVehicleHistory[TramRefPoints] = self._get_history_for_vehicle(vehicle_id)
        for idx in range(len(history)): # type: int
            # look at the `idx+1`-th vehicle info from the end
            idx_from_end: int = - (idx + 1)
            # get the assigned zone ids
            # zone_ids: Set[str] = history[idx_from_end].zone_ids
            zone_ids: Set[str] = history.get_nth_item(idx_from_end).zone_ids
            # get rail track ids
            track_ids: Set[str] = set(filter(lambda zone_id: self._is_track_id(zone_id),
                                             zone_ids))
            # ensure length constraints (0 or 1)
            self._ensure_track_ids_is_singleton(track_ids)
            if len(track_ids) == 0:
                # no track id was assigned to the vehicle at that step;
                # go one step back
                continue
            # track id found -- return it
            return track_ids.pop()
        # either no history yet or no rail track assigned at all inside the history window;
        # return None
        return None

    def _select_track_id_from_withmaxarea(self, *, vehicle_id: str, ids_with_max_area: Set[str]) -> str | None:
        # If there is more than one rail track ID with max area,
        # select the one to which this vehicle was last assigned (search for assignments in history).
        if len(ids_with_max_area) == 0:
            # no track assignment (no intersections)
            return None
        if len(ids_with_max_area) == 1:
            # a single track assignment -- return it
            # this should happen almost all the time
            return next(iter(ids_with_max_area))
        # more than one id in the set:
        # - get the last rail track id assigned to this vehicle inside the history window
        last_assigned_track_id: str | None = self._get_last_assigned_nonnull_track_id(vehicle_id)
        # - check whether it is not null and is among the passed ids
        if last_assigned_track_id is not None and last_assigned_track_id in ids_with_max_area:
            # if it is, return it;
            # rationale: do not change the previously assigned track id in such cases
            return last_assigned_track_id
        # otherwise, no choice left other than to assign one randomly from among the passed ids
        # TODO: a better way?
        randomly_chosen: str = next(iter(ids_with_max_area))
        return randomly_chosen

    def _get_centre_worldplane_refpoint(self, vertices_pos_img: List[PlanarPosition]) -> PositionContainer | None:
        """
        For a tram, find the midpoint (in the **world** plane) of the track's centreline segment
        covered by the vehicle's bounding box, and return the corresponding `PositionContainer`.
        If the image-world coordinate conversion is disabled, return `None`.

        :param vertices_pos_img: the coordinates of vertices defining the centreline segment
          inside the vehicle's bounding box

        :return: the calculated container for the segment's midpoint in world plane terms,
          or `None` if coordinate conversion is disabled
        """
        # centre refpoint in world plane: the midpoint (in WORLD plane terms)
        # of the centreline segment (the same one found above, but defined in WORLD coordinates)
        if not self._with_world_coords:
            return None
        vertices_pos_world: List[PlanarPosition] | None = self._image_to_world_coord_list(vertices_pos_img)
        if vertices_pos_world is None:
            raise RuntimeError("Could not convert vertices from image to world coordinates "
                               "despite _with_world_coords being set to True")
        centreline_inside_bbox_world: LineString = LineString(vertices_pos_world)
        centre_worldplane_refpoint_pt_world: Point = centreline_inside_bbox_world.interpolate(distance=0.5,
                                                                                              normalized=True)
        centre_worldplane_refpoint_world: PlanarPosition = ensure_is_planar_position(
            get_point_coords(centre_worldplane_refpoint_pt_world)
        )
        centre_worldplane_refpoint_img: PlanarPosition | None = self._world_to_image_coord(
            centre_worldplane_refpoint_world
        )
        if centre_worldplane_refpoint_img is None:
            # unexpected behaviour
            raise RuntimeError("Could not convert the world-plane centre reference point from world to image coordinates: "
                               "got None from _world_to_image_coord(), but _with_world_coords is set to True")
        centre_worldplane_refpoint: PositionContainer = PositionContainer(
            image=centre_worldplane_refpoint_img, world=centre_worldplane_refpoint_world
        )
        return centre_worldplane_refpoint

    def _calculate_centreline_refpoints(
            self, bbox_polygon: Polygon, refpoints_track: RailTrack
    ) -> TrackCentrelinePositions | None:
        # the intersection of the bbox (as polygon) and the rail track centreline
        centreline_inside_bbox_img: BaseGeometry = sh.intersection(bbox_polygon, refpoints_track.centreline.image)
        # the intersection of the bbox's border and the rail track centreline
        centreline_bbox_border_intersection: BaseGeometry = sh.intersection(
            bbox_polygon.boundary, refpoints_track.centreline.image
        )

        # --- constraint checks ---
        # For centreline-based reference points to make sense, ...
        if not isinstance(centreline_inside_bbox_img, LineString) or centreline_inside_bbox_img.is_empty:
            # (1) The intersection must be a non-empty linestring;
            # if this is not the case, return `None`.
            return None
        # check that the centreline intersects the bbox's border in at least two points
        if centreline_bbox_border_intersection.is_empty or isinstance(centreline_bbox_border_intersection, Point):
            # (2) The segment of the track's centreline that is inside the vehicle's bounding box
            # must intersect it in at least two points; if this is not the case, return `None`.
            return None

        # vertices of the contained centreline segment (as positions)
        vertices_pos_img: List[PlanarPosition] = [
            ensure_is_planar_position(coords)
            for coords in list(centreline_inside_bbox_img.coords)
        ]
        # same as points
        vertices_pts_img: List[Point] = [Point(pos) for pos in vertices_pos_img]

        # --- start / end reference points ---
        # find the vertices' distances from the track centreline's start (in image plane)
        vertex_pos_along_track_imgplane: List[float] = [
            refpoints_track.centreline.image.project(pt)
            for pt in vertices_pts_img
        ]

        # Sort by distance ascending, then by the original order or the vertices ascending,
        # then choose the first element as the start and last element as the end.
        # NOTE: In principle, it is expected that there will not be repeating values in `vertex_distances`,
        # but this approach is still being employed to account for this edge case.

        # (vertex_idx, distance)
        vertex_idx_and_pos_along_track: List[Tuple[int, float]] = [
            (idx, dist) for idx, dist in enumerate(vertex_pos_along_track_imgplane)
        ]
        # sort
        vertex_idx_and_pos_along_track.sort(key=lambda elem: (elem[1], elem[0]), reverse=False)
        # first: start
        start_vertex_idx, start_pos_along_track_imgplane = vertex_idx_and_pos_along_track[0] # type: int, float
        # second: end
        end_vertex_idx, end_pos_along_track_imgplane = vertex_idx_and_pos_along_track[-1]  # type: int, float

        start_refpoint_img: PlanarPosition = vertices_pos_img[start_vertex_idx]
        end_refpoint_img: PlanarPosition = vertices_pos_img[end_vertex_idx]

        # --- centre reference point (in IMAGE plane) ---
        # definition: the midpoint (in image plane terms) of the centreline segment inside the bounding box

        # find the midpoint
        centre_imgplane_refpoint_pt_img: Point = centreline_inside_bbox_img.interpolate(distance=0.5,
                                                                                        normalized=True)
        # extract position
        centre_imgplane_refpoint_img: PlanarPosition = ensure_is_planar_position(
            get_point_coords(centre_imgplane_refpoint_pt_img)
        )

        # ... convert the calculated refpoints to world coordinates ...
        start_refpoint_world: PlanarPosition | None = self._image_to_world_coord(start_refpoint_img)
        end_refpoint_world: PlanarPosition | None = self._image_to_world_coord(end_refpoint_img)
        centre_imgplane_refpoint_world: PlanarPosition | None = self._image_to_world_coord(centre_imgplane_refpoint_img)

        # ... construct objects ...
        start_refpoint: PositionContainer = PositionContainer(image=start_refpoint_img,
                                                              world=start_refpoint_world)
        end_refpoint: PositionContainer = PositionContainer(image=end_refpoint_img,
                                                            world=end_refpoint_world)
        centre_imgplane_refpoint: PositionContainer = PositionContainer(image=centre_imgplane_refpoint_img,
                                                                        world=centre_imgplane_refpoint_world)

        # --- centre reference point (in WORLD plane) ---
        # definition: see `_get_centre_worldplane_refpoint()`
        centre_worldplane_refpoint: PositionContainer | None = self._get_centre_worldplane_refpoint(vertices_pos_img)

        # --- output object ---
        ref_points: TrackCentrelinePositions = TrackCentrelinePositions(
            start=start_refpoint, end=end_refpoint,
            centre_in_image_plane=centre_imgplane_refpoint,
            centre_in_world_plane=centre_worldplane_refpoint
        )

        return ref_points

    def _calculate_ref_points(self, bbox_polygon: Polygon, refpoints_track: RailTrack | None) -> TramRefPoints:
        centroid, lower_border_midpoint = self._calculate_base_refpoint_positions(bbox_polygon) # type: PositionContainer, PositionContainer
        centreline_refpoints: TrackCentrelinePositions | None = (
            self._calculate_centreline_refpoints(bbox_polygon, refpoints_track)
            if refpoints_track is not None
            else None
        )
        container: TramRefPoints = TramRefPoints(bbox_centroid=centroid,
                                                 bbox_lower_border_midpoint=lower_border_midpoint,
                                                 vehicle_centreline=centreline_refpoints)
        return container

    def _get_refpoint_pos_for_platform_assignment(
            self, tram_centreline_points: TrackCentrelinePositions
    ) -> PlanarPosition:
        """
        Get the reference point to be used for platform assignment:
        - If this instance's `_with_world_coords` is set to `True`: the reference point for the centre wrt the **world plane**, in **image coordinates**.
        - Otherwise: the reference point for the centre wrt the **image plane** (likewise in **image coordinates**).
        """
        if not self._with_world_coords:
            return tram_centreline_points.centre_in_image_plane.image
        worldplane_centre_pos_container: PositionContainer | None = tram_centreline_points.centre_in_world_plane
        if worldplane_centre_pos_container is None:
            raise RuntimeError("Got invalid reference points for a tram (missing data) "
                               "with this instance's _with_world_coords is set to True")
        pos: PlanarPosition | None = worldplane_centre_pos_container.image
        if pos is None:
            raise RuntimeError("Got invalid reference points for a tram (missing data) "
                               "with this instance's _with_world_coords is set to True")
        return pos


    def _is_on_platform(self, ref_points: TramRefPoints, platform: RailPlatform) -> bool:
        centreline_points: TrackCentrelinePositions | None = ref_points.vehicle_centreline
        if centreline_points is None:
            # if no centreline points are defined, then the tram is not on a track at all,
            # and is consequently also not on a platform
            return False
        refpoint_pos: PlanarPosition = self._get_refpoint_pos_for_platform_assignment(centreline_points)
        refpoint: Point = Point(refpoint_pos)
        # compare the distance from the reference point to the platform line
        # by comparing it to the tolerance threshold
        # (to account for floating-point rounding errors)
        return point_is_in_geometry(pt=refpoint,
                                    geometry=platform.polyline.image,
                                    tolerance=SHAPELY_POINT_ON_LINE_DISTANCE_TOLERANCE_IMAGECOORDS)

    def _assign_platform_ids(self, track_id: str, ref_points: TramRefPoints) -> Set[str]:
        # The ids of all platforms on this rail track.
        platform_ids: Set[str] = self._zones.track_to_platforms[track_id]
        # The ids of platforms to which this vehicle is assigned by `_is_on_platform()`
        # (current implementation: the vehicle's centre reference point,
        # as defined in the world plane if this instance's `_with_world_coords` property
        # is set to `True` or in the image plane otherwise, must be inside the platform zone).
        filtered: Set[str] = {
            platform_id for platform_id in platform_ids
            if self._is_on_platform(ref_points, self._zones.platforms[platform_id])
        }
        return filtered

    @override
    def _calculate_zones_and_ref_points(self, state: TrackState) -> Tuple[Set[str], TramRefPoints]:
        # --- TRACK ASSIGNMENT ---
        # (1) intersect bbox with all zones, find argmax of zone area inside bbox
        # (2) compute the ref points based on this assignment
        # (3) determine whether the centre ref point (world coords if available, image otherwise)
        #     is inside the track zone -> assign rail track id to the vehicle if yes, null otherwise
        # (4) result:
        #     - the single track id (wrapped in a set) / empty set based on (3);
        #     - the ref points from (2) irrespective of the result of (3).
        # --- PLATFORM ASSIGNMENT ---
        # determine whether the centre ref point is inside any of the platforms belonging to this track zone
        # return the set of platform ids accordingly
        #
        # return: { rail and platform track ids }, ref_points

        vehicle_id: str = state.track_id

        # --- TRACK ASSIGNMENT ---
        bbox_polygon: Polygon = _bbox_to_polygon(state.bbox)
        # track ids for tracks with the largest bbox area inside the track
        assigned_track_id, ref_points = self._calculate_rail_track_id_and_ref_points(
            vehicle_id=vehicle_id, bbox_polygon=bbox_polygon
        ) # type: str | None, TramRefPoints

        # Currently, returning as track IDs a set containing just the single one
        # that was used for reference point calculation (it would make little sense
        # to also return the other track IDs in this scenario).
        #
        # When the system will incorporate handling rail track transition for a single tram,
        # perhaps the zone IDs of both rail tracks will need to be returned
        # (e. g. when the tram's forwardmost car is already on track A,
        # whilst its backwardmost car is yet on track B).
        assigned_track_ids: Set[str] = {assigned_track_id} if assigned_track_id is not None else set()

        # --- PLATFORM ASSIGNMENT ---
        assigned_platform_ids: Set[str] = (
            self._assign_platform_ids(assigned_track_id, ref_points)
            if assigned_track_id is not None
            else set()
        )

        zone_ids: Set[str] = set.union(assigned_track_ids, assigned_platform_ids)

        return zone_ids, ref_points

    @override
    def _select_ref_point_container_for_speed_calculation(self, ref_points: TramRefPoints) -> PositionContainer | None:
        # Assumption:
        # These reference points are for a position that has already been selected to be included
        # in the history to be passed to the speed estimator.
        # For trams, this means that there will NOT be a container
        # in which the centreline reference points are undefined.
        # If this happens, this is an issue that has to be solved in the filtering code, not here.
        centreline_points: TrackCentrelinePositions | None = ref_points.vehicle_centreline
        if centreline_points is None:
            return None
        if self._with_world_coords:
            container_worldplane: PositionContainer | None = centreline_points.centre_in_world_plane
            if container_worldplane is None:
                msg: str = ("Received a reference point container for a tram after filtering -- "
                            "the world plane position is expected to be defined with this instance's "
                            "_with_world_coords set to True, but is null")
                raise RuntimeError(msg)
            return container_worldplane
        return centreline_points.centre_in_image_plane

    @staticmethod
    def _check_all_centreline_refpoints_defined(filtered: List[BaseInfoForSpeedCalculation[TramRefPoints]]) -> None:
        if any(item.reference_points.vehicle_centreline is None
               for item in filtered):
            raise ValueError("Received a sequence of reference point containers for a tram "
                             "with undefined centreline reference points in at least one position -- unexpected")

    @override
    def _filter_history_for_speed_calculation(
            self, history: List[BaseInfoForSpeedCalculation[TramRefPoints]]
    ) -> List[BaseInfoForSpeedCalculation[TramRefPoints]]:
        # TRAMS:
        #
        # Select only the latest continuous period with the same rail track ID.
        # If the last track ID is null, return an empty list.
        #
        # Motivation:
        # (1) One of the centreline reference points is used for speed estimation.
        #   These points are currently only expected to stay consistent
        #   whilst the assigned rail track remains the same.
        # (2) For null assignments, the centreline reference points are not defined
        #   (because there is no track in relation to which to define them).

        prev_track_id: str | None = None
        track_id_changed_on_idx: int | None = None
        for idx, item in enumerate(history): # type: int, BaseInfoForSpeedCalculation[TramRefPoints]
            zone_ids: Set[str] = item.zone_ids
            track_ids: List[str] = list(filter(lambda zone_id: self._is_track_id(zone_id), zone_ids))
            # in the current implementation, the length can only be 0 or 1
            if len(track_ids) > 1:
                raise RuntimeError("Got a set of rail track ID assignments for a single state that is longer than one")
            cur_track_id: str | None = track_ids[0] if len(track_ids) == 1 else None
            if cur_track_id is None:
                # reset the stored index
                track_id_changed_on_idx = None
                prev_track_id = None
            else:
                # current track id is non-null
                if prev_track_id is None or cur_track_id != prev_track_id:
                    # change of track id --> update the stored index
                    track_id_changed_on_idx = idx
                    prev_track_id = cur_track_id


        # NOTE: null-based slicing works fine (will produce an empty collection),
        # but still explicitly creating an empty array for intuitiveness
        filtered: List[BaseInfoForSpeedCalculation[TramRefPoints]] = (
            history[track_id_changed_on_idx:] if track_id_changed_on_idx is not None
            else []
        )
        # ADDITIONAL CHECK HERE:
        # Checking that the centreline reference points at this position are defined.
        # If they are not, that is a logical error (for a defined rail track,
        # the centreline reference points should always be defined).
        self._check_all_centreline_refpoints_defined(filtered)
        return filtered

    @override
    @staticmethod
    def _build_info_for_speed_calculation(frame_ts: datetime,
                                          state: TrackState,
                                          zone_ids: Set[str],
                                          ref_points: TramRefPoints) -> TramInfoForSpeedCalculation:
        return TramInfoForSpeedCalculation(
            frame_ts=frame_ts, state=state, zone_ids=zone_ids, reference_points=ref_points
        )

    @override
    def _create_history_for_new_vehicle(self) -> BaseVehicleHistory[TramRefPoints]:
        return TramHistory(maxlen=self._max_history_per_vehicle)

    @override
    @staticmethod
    def _build_vehicle_history_item(*, frame_ts: datetime,
                                    state: TrackState,
                                    zone_ids: Set[str], ref_points: TramRefPoints,
                                    speeds: Speeds) -> TramHistoryItem:
        return TramHistoryItem(
            frame_ts=frame_ts, state=state, zone_ids=zone_ids, speeds=speeds, reference_points=ref_points
        )

    @override
    @staticmethod
    def _build_vehicle_info(
            *, vehicle_id: str, history_item: BaseVehicleHistoryItem[TramRefPoints]
    ) -> TramInfo:
        return TramInfo(
            vehicle_id=vehicle_id,
            frame_ts=history_item.frame_ts,
            is_matched=history_item.state.is_matched,
            zone_ids=history_item.zone_ids,
            reference_points=history_item.reference_points,
            speeds=history_item.speeds
        )
