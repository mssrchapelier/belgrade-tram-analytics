from abc import ABC, abstractmethod
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime
from itertools import chain
from typing import (
    List, Dict, Set, Tuple, DefaultDict, TypeGuard, Any, Deque,
    override, Iterator, Iterable
)

import shapely as sh
from shapely import Polygon, LineString, Point
from shapely.geometry.base import BaseGeometry

from common.utils.custom_types import PlanarPosition, is_planar_position, ensure_is_planar_position
from common.utils.data_structures.alive_obj_manager import AliveObjectHistoryManager
from common.utils.shapely_utils import get_linestring_segment, get_point_coords, point_is_in_geometry
from tram_analytics.v1.models.common_types import VehicleType, BoundingBox
from tram_analytics.v1.models.components.tracking import TrackState
from tram_analytics.v1.models.components.vehicle_info import (
    PositionContainer, BaseRefPoints, TramRefPoints, CarRefPoints,
    TrackCentrelinePositions, Speeds, VehicleInfo, TramInfo, CarInfo
)
from tram_analytics.v1.pipeline.components.vehicle_info.coord_conversion.homography import CoordConverter
from tram_analytics.v1.pipeline.components.vehicle_info.coord_conversion.homography_config import HomographyConfig
from tram_analytics.v1.pipeline.components.vehicle_info.settings import (
    SHAPELY_POINT_ON_LINE_DISTANCE_TOLERANCE_IMAGECOORDS,
    MAX_VEHICLE_HISTORY_SIZE
)
from tram_analytics.v1.pipeline.components.vehicle_info.speeds.config import SpeedCalculatorConfig
from tram_analytics.v1.pipeline.components.vehicle_info.speeds.speeds import TimedPosition, SpeedCalculator
from tram_analytics.v1.pipeline.components.vehicle_info.zones.zones_config import (
    BasePolygonZoneCoordsConfig,
    RailPlatformEndpointSupportingLine, BaseZoneTypeSettings,
    BaseZoneSetConfig, IntrusionZoneConfig, RailTrackConfig, RailPlatformConfig, ZonesConfig,
    IntrusionZoneCoordsConfig, RailPlatformCoordsConfig, RailTrackCoordsConfig,
    IntrusionZoneSettings, BaseSingleZoneConfig
)


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

# --- zone assignment objects ---

# --- (1) base ---

class BaseZoneAndSpeedAssigner[RefPoints: BaseRefPoints, VehicleInfoType: VehicleInfo](ABC):

    _msg_invalid_state_world_converter: str = "Invalid state: _with_world_coords is True but _coord_converter is undefined"

    def __init__(self, *, coord_converter: CoordConverter | None,
                 speed_config: SpeedCalculatorConfig) -> None:

        # Whether image/world coordinate conversion is enabled.
        self._with_world_coords: bool = coord_converter is not None

        # The object handling image/world coordinate conversion (`None` if disabled).
        self._coord_converter: CoordConverter | None = coord_converter

        self._speed_calculator: SpeedCalculator = SpeedCalculator(speed_config)

        # The history manager that stores custom history (of type `ZoneAssignmentHistoryItem`)
        # for every vehicle ID that is currently alive.
        # At each step, histories for new IDs are initialised, and the ones for the dead IDs are discarded.
        self._max_history_per_vehicle: int = MAX_VEHICLE_HISTORY_SIZE
        # self._history_manager: AliveObjectHistoryManager[
        #     Deque[BaseVehicleHistoryItem[RefPoints]]
        # ] = AliveObjectHistoryManager(self._create_history_for_new_vehicle)

        self._history_manager: AliveObjectHistoryManager[
            BaseVehicleHistory[RefPoints]
        ] = AliveObjectHistoryManager(self._create_history_for_new_vehicle)


    # --- master ---

    def process_for_frame(self, *, states: List[TrackState], frame_ts: datetime) -> List[VehicleInfoType]:
        alive_ids: Set[str] = {state.track_id for state in states}
        if len(alive_ids) != len(states):
            raise ValueError("For any frame, no two track states can have the same track IDs (vehicle ID), "
                             "but found duplicates")
        self._history_manager.update_ids(alive_ids)
        results: List[VehicleInfoType] = []
        for state in states: # type: TrackState
            vehicle_id: str = state.track_id
            history_item: BaseVehicleHistoryItem[RefPoints] = self._calculate_for_vehicle(track_state=state,
                                                                                          timestamp=frame_ts)
            self._update_history_for_vehicle(vehicle_id=vehicle_id,
                                             history_item=history_item)
            # result: BaseVehicleInfo[RefPoints] = self._build_vehicle_info(vehicle_id=vehicle_id,
            #                                                               history_item=history_item)
            result: VehicleInfoType = self._build_vehicle_info(vehicle_id=vehicle_id,
                                                               history_item=history_item)
            results.append(result)
        return results

    # --- vehicle info calculation methods ---

    # (1) master for one vehicle

    def _calculate_for_vehicle(self, *, track_state: TrackState, timestamp: datetime) -> BaseVehicleHistoryItem[RefPoints]:
        vehicle_id: str = track_state.track_id
        # prev_history: Deque[BaseVehicleHistoryItem[RefPoints]] = self._get_history_for_vehicle(vehicle_id)
        prev_history: BaseVehicleHistory[RefPoints] = self._get_history_for_vehicle(vehicle_id)
        zone_ids, ref_points = self._calculate_zones_and_ref_points(track_state)  # type: Set[str], RefPoints
        cur_info: BaseInfoForSpeedCalculation[RefPoints] = self._build_info_for_speed_calculation(
            frame_ts=timestamp, state=track_state, zone_ids=zone_ids, ref_points=ref_points
        )
        speeds: Speeds = self._calculate_speeds(cur_info=cur_info,
                                                prev_history=prev_history)
        history_item: BaseVehicleHistoryItem[RefPoints] = self._build_vehicle_history_item(
            frame_ts=timestamp, state=track_state, zone_ids=zone_ids, ref_points=ref_points,
            speeds=speeds
        )
        return history_item

    # (2) zone assignment and reference point calculation

    @abstractmethod
    def _calculate_zones_and_ref_points(self, state: TrackState) -> Tuple[Set[str], RefPoints]:
        """
        Given a state, returns:
        (1) the assigned zone IDs;
        (2) the calculated reference points.
        """
        pass

    def _calculate_base_refpoint_positions(self, bbox: Polygon) -> Tuple[PositionContainer, PositionContainer]:
        """
        Calculate bounding box-based reference points (centroid, lower border midpoint).
        """
        bbox_x1, bbox_y1, bbox_x2, bbox_y2 = bbox.bounds # type: float, float, float, float
        centroid_x: float = bbox_x1 + (bbox_x2 - bbox_x1) / 2.0
        centroid_y: float = bbox_y1 + (bbox_y2 - bbox_y1) / 2.0
        centroid_img: PlanarPosition = (centroid_x, centroid_y)
        lower_border_midpoint_img: PlanarPosition = (centroid_x, bbox_y2)

        centroid_world: PlanarPosition | None = self._image_to_world_coord(centroid_img)
        lower_border_midpoint_world: PlanarPosition | None = self._image_to_world_coord(lower_border_midpoint_img)

        centroid: PositionContainer = PositionContainer(image=centroid_img,
                                                        world=centroid_world)
        lower_border_midpoint: PositionContainer = PositionContainer(image=lower_border_midpoint_img,
                                                                     world=lower_border_midpoint_world)
        return centroid, lower_border_midpoint

    # (3) speed calculation

    def _get_last_timed_pos_from_history(self, history: BaseVehicleHistory[RefPoints]) -> TimedPosition | None:
        last_item: BaseVehicleHistoryItem[RefPoints] | None = history.get_last_item()
        if last_item is None:
            return None
        ts: datetime = last_item.frame_ts
        pos: PlanarPosition | None = self._select_ref_point_for_speed_calculation(last_item.reference_points)
        return TimedPosition(ts=ts, position=pos) if pos is not None else None

    def _extract_timed_positions_from_filtered_history(
            self, filtered: Iterable[BaseInfoForSpeedCalculation[RefPoints]]
    ) -> List[TimedPosition]:
        timed_positions: List[TimedPosition] = []
        for item in filtered: # type: BaseInfoForSpeedCalculation[RefPoints]
            refpoint_pos: PlanarPosition | None = self._select_ref_point_for_speed_calculation(item.reference_points)
            if refpoint_pos is None:
                # This should not happen after filtering out positions where the vehicle was not assigned to any track
                msg: str = (
                    "Received a reference point container for a tram after filtering -- "
                    "the centreline reference points sub-container is expected to be defined, but is null"
                )
                raise RuntimeError(msg)
            timed_pos: TimedPosition = TimedPosition(ts=item.frame_ts,
                                                     position=refpoint_pos)
            timed_positions.append(timed_pos)
        return timed_positions

    def _calculate_speeds(self, *, cur_info: BaseInfoForSpeedCalculation[RefPoints],
                          prev_history: BaseVehicleHistory[RefPoints]) -> Speeds:

        # for raw speed calculation: build previous (if exists) and current positions
        prev_timed_pos: TimedPosition | None = self._get_last_timed_pos_from_history(prev_history)
        cur_refpoint_pos: PlanarPosition | None = self._select_ref_point_for_speed_calculation(
            cur_info.reference_points
        )
        # cur_refpoint_pos is None if no rail track has been assigned to the tram
        cur_timed_pos: TimedPosition | None = TimedPosition(
            ts=cur_info.frame_ts,
            position=cur_refpoint_pos
        ) if cur_refpoint_pos is not None else None

        # for smoothed speed calculation: build eligible history
        history_all: List[BaseInfoForSpeedCalculation[RefPoints]] = [
            self._build_info_for_speed_calculation(
                frame_ts=item.frame_ts, state=item.state,
                zone_ids=item.zone_ids, ref_points=item.reference_points
            )
            for item in prev_history
        ]
        history_all.append(cur_info)

        # Trim from the beginning as necessary.
        #
        # This is needed at the moment, e. g. to select only the relevant history after a rail track change.
        # This trimming should be removed once the algorithm for reference point calculation
        # ensures its independence from such events.
        #
        # TODO: remove this step in the future
        filtered: List[BaseInfoForSpeedCalculation[RefPoints]] = self._filter_history_for_speed_calculation(
            history_all
        )

        smoothing_history: List[TimedPosition] = self._extract_timed_positions_from_filtered_history(filtered)

        speeds: Speeds = self._speed_calculator.calculate_speeds(
            prev_pos=prev_timed_pos, cur_pos=cur_timed_pos, smoothing_history=smoothing_history
        )
        return speeds

    @abstractmethod
    def _filter_history_for_speed_calculation(
            self, history: List[BaseInfoForSpeedCalculation[RefPoints]]
    ) -> List[BaseInfoForSpeedCalculation[RefPoints]]:
        """
        Filter the passed history (containing also the current information)
        to that to be considered for speed smoothing purposes, trimming it from the beginning if necessary.
        The sliding window may then result to be shorter than this part of the history, but will not exceed it.

        Meant to be used to filter earlier observations that should not be used for speed smoothing
        because it would not make sense to include them, mainly to account for abrupt jumps
        associated with the positions, e. g. after a rail track change).

        The default implementation returns `None` if an empty history was provided,
        and 0 otherwise (reflecting the fact that the entire history can be considered).

        NOTE:
          Once the system evolves to prevent such jumps by design, this functionality
          can be removed and the smoothing window size can be calculated over the entire provided
          observation history.
        """
        pass

    @abstractmethod
    def _select_ref_point_container_for_speed_calculation(self, ref_points: RefPoints) -> PositionContainer | None:
        pass

    def _select_ref_point_for_speed_calculation(self, ref_points: RefPoints) -> PlanarPosition | None:
        container: PositionContainer | None = self._select_ref_point_container_for_speed_calculation(ref_points)
        if container is None:
            return None
        # use the world coordinates for speed estimation (if enabled)
        if self._with_world_coords:
            pos_world: PlanarPosition | None = container.world
            if pos_world is None:
                # by design, should not happen
                msg: str = "Expected a numerical world coordinate for the reference point with _with_world_coords set to True, but got null"
                raise RuntimeError(msg)
            return pos_world
        # otherwise, use image coordinates
        return container.image

    # --- history handling helpers ---

    # def _create_history_for_new_vehicle(self) -> Deque[BaseVehicleHistoryItem[RefPoints]]:
    #     """
    #     A factory method to create a new `Deque[VehicleHistoryItem]`. Used internally by `AliveObjectHistoryManager`.
    #     """
    #     return deque(maxlen=self._max_history_per_vehicle)

    @abstractmethod
    def _create_history_for_new_vehicle(self) -> BaseVehicleHistory[RefPoints]:
        """
        A factory method to create a new `Deque[VehicleHistoryItem]`. Used internally by `AliveObjectHistoryManager`.
        """
        pass

    # def _get_history_for_vehicle(self, vehicle_id: str) -> Deque[BaseVehicleHistoryItem[RefPoints]]:
    #     history: Deque[BaseVehicleHistoryItem[RefPoints]] = self._history_manager[vehicle_id]
    #     return history

    def _get_history_for_vehicle(self, vehicle_id: str) -> BaseVehicleHistory[RefPoints]:
        history: BaseVehicleHistory[RefPoints] = self._history_manager[vehicle_id]
        return history

    # def _get_last_history_item_for_vehicle(self, vehicle_id: str) -> BaseVehicleHistoryItem[RefPoints] | None:
    #     history: Deque[BaseVehicleHistoryItem[RefPoints]] = self._get_history_for_vehicle(vehicle_id)
    #     return history[-1] if len(history) > 0 else None

    # def _update_history_for_vehicle(
    #         self, *, vehicle_id: str, history_item: BaseVehicleHistoryItem[RefPoints]
    # ) -> None:
    #     """
    #     Update the history item for this vehicle ID.
    #     """
    #     vehicle_history: Deque[BaseVehicleHistoryItem[RefPoints]] = self._get_history_for_vehicle(vehicle_id)
    #     vehicle_history.append(history_item)

    def _update_history_for_vehicle(
            self, *, vehicle_id: str, history_item: BaseVehicleHistoryItem[RefPoints]
    ) -> None:
        """
        Update the history item for this vehicle ID.
        """
        vehicle_history: BaseVehicleHistory[RefPoints] = self._get_history_for_vehicle(vehicle_id)
        vehicle_history.add(history_item)

    # --- shape building helpers ---

    def _build_polygon_container(self, polygon_coords_img: List[PlanarPosition]) -> PolygonContainer:
        shape_image: Polygon = Polygon(polygon_coords_img)
        pts_world: List[PlanarPosition] | None = self._image_to_world_coord_list(polygon_coords_img)
        shape_world: Polygon | None = Polygon(pts_world) if self._with_world_coords else None
        container: PolygonContainer = PolygonContainer(image=shape_image, world=shape_world)
        return container

    def _build_polyline_container(self, polyline_coords_img: List[PlanarPosition]) -> PolylineContainer:
        shape_image: LineString = LineString(polyline_coords_img)
        pts_world: List[PlanarPosition] | None = self._image_to_world_coord_list(polyline_coords_img)
        shape_world: LineString | None = LineString(pts_world) if self._with_world_coords else None
        container: PolylineContainer = PolylineContainer(image=shape_image, world=shape_world)
        return container

    # --- coordinate conversion helpers ---

    def _image_to_world_coord(self, img_coord: PlanarPosition) -> PlanarPosition | None:
        if self._with_world_coords:
            if self._coord_converter is None:
                raise RuntimeError(self._msg_invalid_state_world_converter)
            return self._coord_converter.image_to_world([img_coord])[0]
        return None

    def _image_to_world_coord_list(self, img_coords: List[PlanarPosition]) -> List[PlanarPosition] | None:
        if self._with_world_coords:
            if self._coord_converter is None:
                raise RuntimeError(self._msg_invalid_state_world_converter)
            return self._coord_converter.image_to_world(img_coords)
        return None

    def _world_to_image_coord(self, world_coord: PlanarPosition) -> PlanarPosition | None:
        if self._with_world_coords:
            if self._coord_converter is None:
                raise RuntimeError(self._msg_invalid_state_world_converter)
            return self._coord_converter.world_to_image([world_coord])[0]
        return None

    def _world_to_image_coord_list(self, world_coords: List[PlanarPosition]) -> List[PlanarPosition] | None:
        if self._with_world_coords:
            if self._coord_converter is None:
                raise RuntimeError(self._msg_invalid_state_world_converter)
            return self._coord_converter.world_to_image(world_coords)
        return None

    # --- object building helpers ---

    @staticmethod
    @abstractmethod
    def _build_info_for_speed_calculation(frame_ts: datetime,
                                          state: TrackState,
                                          zone_ids: Set[str],
                                          ref_points: RefPoints) -> BaseInfoForSpeedCalculation[RefPoints]:
        pass

    @staticmethod
    @abstractmethod
    def _build_vehicle_history_item(*, frame_ts: datetime,
                                    state: TrackState,
                                    zone_ids: Set[str], ref_points: RefPoints,
                                    speeds: Speeds) -> BaseVehicleHistoryItem[RefPoints]:
        pass

    @staticmethod
    @abstractmethod
    def _build_vehicle_info(
            *, vehicle_id: str, history_item: BaseVehicleHistoryItem[RefPoints]
    ) -> VehicleInfoType:
        pass


class BaseBboxBasedZoneAndSpeedAssigner[
    RefPoints: BaseRefPoints,
    ZCConfig: BasePolygonZoneCoordsConfig,
    ZTSettings: BaseZoneTypeSettings,
    Zone: BaseZone,
    VehicleInfoType: VehicleInfo
](BaseZoneAndSpeedAssigner[RefPoints, VehicleInfoType]):

    def __init__(self, *, coord_converter: CoordConverter | None,
                 speed_config: SpeedCalculatorConfig,
                 zones_config: BaseZoneSetConfig[ZCConfig, ZTSettings]
                 ) -> None:
        # zone id -> zone object
        super().__init__(coord_converter=coord_converter, speed_config=speed_config)
        self._zones: Dict[str, Zone] = self._build_zones(zones_config)

    @abstractmethod
    def _build_zones(self, zones_config: BaseZoneSetConfig[ZCConfig, ZTSettings]) -> Dict[str, Zone]:
        """
        Creates zone objects from configs, constructs a `Dict`
        mapping zone IDs to the created objects and returns the mappings.
        """
        pass

# --- (2) implementations ---

class CarZoneAndSpeedAssigner(
    BaseBboxBasedZoneAndSpeedAssigner[
        CarRefPoints, IntrusionZoneCoordsConfig, IntrusionZoneSettings, IntrusionZone, CarInfo
    ]
):

    def __init__(self, *,
                 zones_config: IntrusionZoneConfig,
                 coord_converter: CoordConverter | None,
                 speed_config: SpeedCalculatorConfig) -> None:

        super().__init__(zones_config=zones_config,
                         coord_converter=coord_converter,
                         speed_config=speed_config)
        # The minimum fraction of the area of a vehicle's bounding box that needs to be inside the zone's polygon
        # for the zone to be assigned to this vehicle (see definition in `IntrusionZoneSettings`).
        self._min_area_frac_inside_zone: float = zones_config.assignment_settings.min_area_frac_inside_zone

    @override
    def _build_zones(
            self, zones_config: BaseZoneSetConfig[IntrusionZoneCoordsConfig, IntrusionZoneSettings]
    ) -> Dict[str, IntrusionZone]:
        zones: Dict[str, IntrusionZone] = dict()
        for zone_cfg in zones_config.zones: # type: BaseSingleZoneConfig[IntrusionZoneCoordsConfig]
            pts_image: List[PlanarPosition] = zone_cfg.coords.polygon
            polygon_container: PolygonContainer = self._build_polygon_container(pts_image)
            zone: IntrusionZone = IntrusionZone(polygon=polygon_container)
            zones[zone_cfg.zone_id] = zone
        return zones

    def _calculate_ref_points(self, bbox_polygon: Polygon) -> CarRefPoints:
        centroid, lower_border_midpoint = self._calculate_base_refpoint_positions(bbox_polygon)
        ref_points: CarRefPoints = CarRefPoints(bbox_centroid=centroid,
                                                bbox_lower_border_midpoint=lower_border_midpoint)
        return ref_points

    @override
    def _calculate_zones_and_ref_points(self, state: TrackState) -> Tuple[Set[str], CarRefPoints]:
        bbox_polygon: Polygon = _bbox_to_polygon(state.bbox)

        # (1) zones: intersect bbox with all zones, threshold by IoU, return all ids over threshold

        # zone id -> the fraction of the bbox's area that is inside the zone polygon
        area_frac_inside_zone: Dict[str, float] = {
            zone_id: sh.intersection(bbox_polygon, zone.polygon.image).area / bbox_polygon.area
            for zone_id, zone in self._zones.items()
        }
        # filter the above by the threshold and store the respective zone ids
        above_threshold_zone_ids: Set[str] = {
            zone_id
            for zone_id, frac in area_frac_inside_zone.items()
            if frac >= self._min_area_frac_inside_zone
        }

        # (2) ref points: bbox-based
        ref_points: CarRefPoints = self._calculate_ref_points(bbox_polygon)
        return above_threshold_zone_ids, ref_points

    @override
    def _select_ref_point_container_for_speed_calculation(self, ref_points: CarRefPoints) -> PositionContainer:
        container: PositionContainer = ref_points.bbox_lower_border_midpoint
        return container

    @override
    def _filter_history_for_speed_calculation(
            self, history: List[BaseInfoForSpeedCalculation[CarRefPoints]]
    ) -> List[BaseInfoForSpeedCalculation[CarRefPoints]]:
        # for cars, return the entire history
        # (speed estimation for them is based on reference points derived
        # from the vehicle's bounding boxes, which are always available,
        # and any temporal inconsistency between them is attributed to the detector and tracker,
        # not to this module)
        return history

    @override
    @staticmethod
    def _build_info_for_speed_calculation(frame_ts: datetime,
                                          state: TrackState,
                                          zone_ids: Set[str],
                                          ref_points: CarRefPoints) -> CarInfoForSpeedCalculation:
        return CarInfoForSpeedCalculation(
            frame_ts=frame_ts, state=state, zone_ids=zone_ids, reference_points=ref_points
        )

    @override
    def _create_history_for_new_vehicle(self) -> BaseVehicleHistory[CarRefPoints]:
        return CarHistory(maxlen=self._max_history_per_vehicle)

    @override
    @staticmethod
    def _build_vehicle_history_item(*, frame_ts: datetime,
                                    state: TrackState,
                                    zone_ids: Set[str], ref_points: CarRefPoints,
                                    speeds: Speeds) -> CarHistoryItem:
        return CarHistoryItem(
            frame_ts=frame_ts, state=state, zone_ids=zone_ids, speeds=speeds, reference_points=ref_points
        )

    @override
    @staticmethod
    def _build_vehicle_info(
            *, vehicle_id: str, history_item: BaseVehicleHistoryItem[CarRefPoints]
    ) -> CarInfo:
        return CarInfo(
            vehicle_id=vehicle_id,
            frame_ts=history_item.frame_ts,
            is_matched=history_item.state.is_matched,
            zone_ids=history_item.zone_ids,
            reference_points=history_item.reference_points,
            speeds=history_item.speeds
        )


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

class ZoneAndSpeedAssigner:

    def __init__(self,
                 *, zones_config: ZonesConfig,
                 homography_config: HomographyConfig | None,
                 speed_config: SpeedCalculatorConfig):
        self._coord_converter: CoordConverter | None = (
            CoordConverter(homography_config) if homography_config is not None
            else None
        )

        self._car_processor: CarZoneAndSpeedAssigner = CarZoneAndSpeedAssigner(
            zones_config=zones_config.intrusion_zones,
            coord_converter=self._coord_converter,
            speed_config=speed_config
        )
        self._tram_processor: TramZoneAndSpeedAssigner = TramZoneAndSpeedAssigner(
            rail_track_config=zones_config.tracks,
            platform_config=zones_config.platforms,
            coord_converter=self._coord_converter,
            speed_config=speed_config
        )

    def _select_assigner(self, vehicle_type: VehicleType) -> CarZoneAndSpeedAssigner | TramZoneAndSpeedAssigner:
        match vehicle_type:
            case VehicleType.CAR:
                return self._car_processor
            case VehicleType.TRAM:
                return self._tram_processor
            case _:
                raise ValueError(f"No zone assigner defined for vehicle type: {vehicle_type}")

    def process_for_frame(
            self, *, states: List[TrackState], frame_ts: datetime
    ) -> List[VehicleInfo]:
        # TODO: run in two threads for the two assigners?
        #
        # Note re potential offloading to two threads:
        # 1) This may not introduce any noticeable speedup since this is all CPU-bound work,
        #   with only small-size NumPy arrays created and worked upon,
        #   so not much is going to be saved by the C code releasing the GIL).
        # 2) Running these in two subprocesses can introduce a greater overhead and limited gains.
        #
        # Leaving as is for now.

        # NOTE: Implemented ensuring that the order of the output objects corresponds to the order of the input states.
        # TODO: Refactor (the below is so unwieldy only due to the reordering)

        # split states by vehicle type and store the original indices
        car_states: List[TrackState] = []
        car_indices: List[int] = []
        tram_states: List[TrackState] = []
        tram_indices: List[int] = []
        for original_idx, state in enumerate(states): # type: int, TrackState
            vehicle_type: VehicleType = state.vehicle_type
            if vehicle_type not in {VehicleType.CAR, VehicleType.TRAM}:
                raise ValueError(f"Unsupported vehicle type: {vehicle_type}")
            append_state_to: List[TrackState] = (
                car_states if vehicle_type is VehicleType.CAR else tram_states
            )
            append_idx_to: List[int] = (
                car_indices if vehicle_type is VehicleType.CAR else tram_indices
            )
            append_state_to.append(state)
            append_idx_to.append(original_idx)
        car_infos: List[VehicleInfo] = []
        tram_infos: List[VehicleInfo] = []
        for vehicle_type, states_for_type in zip([VehicleType.CAR, VehicleType.TRAM],
                                                 [car_states, tram_states]): # type: VehicleType, List[TrackState]
            assigner: CarZoneAndSpeedAssigner | TramZoneAndSpeedAssigner = self._select_assigner(vehicle_type)
            infos_for_type: List[CarInfo] | List[TramInfo] = assigner.process_for_frame(
                states=states_for_type, frame_ts=frame_ts
            )
            extend_which: List[VehicleInfo] = (
                car_infos if vehicle_type is VehicleType.CAR else tram_infos
            )
            extend_which.extend(infos_for_type)
        with_orig_idx: List[Tuple[int, VehicleInfo]] = [
            (idx, info_obj)
            for idx, info_obj in zip(chain(car_indices, tram_indices),
                                     chain(car_infos, tram_infos))
        ]
        # order by the original index
        with_orig_idx.sort(key=lambda item: item[0])
        infos_sorted: List[VehicleInfo] = [info_obj for idx, info_obj in with_orig_idx]
        return infos_sorted
