from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Set, Tuple, Iterable, Dict

from shapely import Polygon, LineString

from common.utils.custom_types import PlanarPosition
from common.utils.data_structures.alive_obj_manager import AliveObjectHistoryManager
from tram_analytics.v1.models.components.tracking import TrackState
from tram_analytics.v1.models.components.vehicle_info import BaseRefPoints, VehicleInfo, Speeds, PositionContainer
from tram_analytics.v1.pipeline.components.vehicle_info.components.containers import (
    BaseVehicleHistory, BaseVehicleHistoryItem, BaseInfoForSpeedCalculation,
    PolygonContainer, PolylineContainer, BaseZone
)
from tram_analytics.v1.pipeline.components.vehicle_info.components.coord_conversion.homography import CoordConverter
from tram_analytics.v1.pipeline.components.vehicle_info.components.speeds.config import SpeedCalculatorConfig
from tram_analytics.v1.pipeline.components.vehicle_info.components.speeds.speeds import SpeedCalculator, TimedPosition
from tram_analytics.v1.pipeline.components.vehicle_info.components.zones.zones_config import (
    BasePolygonZoneCoordsConfig, BaseZoneTypeSettings, BaseZoneSetConfig
)
from tram_analytics.v1.pipeline.components.vehicle_info.settings import MAX_VEHICLE_HISTORY_SIZE


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
            result: VehicleInfoType = self._build_vehicle_info(vehicle_id=vehicle_id,
                                                               history_item=history_item)
            results.append(result)
        return results

    # --- vehicle info calculation methods ---

    # (1) master for one vehicle

    def _calculate_for_vehicle(self, *, track_state: TrackState, timestamp: datetime) -> BaseVehicleHistoryItem[RefPoints]:
        vehicle_id: str = track_state.track_id
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

    @abstractmethod
    def _create_history_for_new_vehicle(self) -> BaseVehicleHistory[RefPoints]:
        """
        A factory method to create a new `Deque[VehicleHistoryItem]`. Used internally by `AliveObjectHistoryManager`.
        """
        pass

    def _get_history_for_vehicle(self, vehicle_id: str) -> BaseVehicleHistory[RefPoints]:
        history: BaseVehicleHistory[RefPoints] = self._history_manager[vehicle_id]
        return history

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
