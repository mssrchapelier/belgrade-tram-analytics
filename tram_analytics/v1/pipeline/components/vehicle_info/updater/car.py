from datetime import datetime
from typing import override, Dict, List, Tuple, Set

import shapely as sh
from shapely import Polygon

from common.utils.custom_types import PlanarPosition
from tram_analytics.v1.models.components.tracking import TrackState
from tram_analytics.v1.models.components.vehicle_info import CarRefPoints, CarInfo, PositionContainer, Speeds
from tram_analytics.v1.pipeline.components.vehicle_info.components.containers import (
    IntrusionZone, PolygonContainer, _bbox_to_polygon, BaseInfoForSpeedCalculation,
    CarInfoForSpeedCalculation, BaseVehicleHistory, CarHistory, CarHistoryItem, BaseVehicleHistoryItem
)
from tram_analytics.v1.pipeline.components.vehicle_info.components.coord_conversion.homography import CoordConverter
from tram_analytics.v1.pipeline.components.vehicle_info.components.speeds.config import SpeedCalculatorConfig
from tram_analytics.v1.pipeline.components.vehicle_info.components.zones.zones_config import (
    IntrusionZoneCoordsConfig, IntrusionZoneSettings, IntrusionZoneConfig, BaseZoneSetConfig, BaseSingleZoneConfig
)
from tram_analytics.v1.pipeline.components.vehicle_info.updater.base import BaseBboxBasedZoneAndSpeedAssigner


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
