from abc import ABC, abstractmethod
from typing import List, override

import shapely
from shapely import Polygon, Point
from shapely.geometry.base import BaseGeometry

from common.utils.shapely_utils import bbox_to_polygon
from tram_analytics.v1.models.components.detection import RawDetection
from tram_analytics.v1.pipeline.components.detection.detection_config import (
    ROIConfig, ROIConfigCentroid, ROIConfigMinAreaFraction
)


class DetectionPostprocessor(ABC):

    @abstractmethod
    def filter(self, detections: List[RawDetection]) -> List[RawDetection]:
        pass


class BaseROIPostprocessor(DetectionPostprocessor):

    def __init__(self):
        self._roi: Polygon = self._build_roi()

    @abstractmethod
    def _build_roi(self) -> Polygon:
        pass

    @abstractmethod
    def _is_in_roi(self, det: RawDetection) -> bool:
        pass

    def filter(self, detections: List[RawDetection]) -> List[RawDetection]:
        """
        Filter detected bounding boxes according to the ROI policy for this instance.
        """
        filtered: List[RawDetection] = list(filter(lambda det: self._is_in_roi(det),
                                                   detections))
        return filtered


class CentroidROIPostprocessor(BaseROIPostprocessor):

    def __init__(self, config: ROIConfigCentroid) -> None:
        self._config: ROIConfig = config
        super().__init__()

    @override
    def _build_roi(self) -> Polygon:
        return Polygon(self._config.coords)

    @override
    def _is_in_roi(self, det: RawDetection) -> bool:
        centroid: Point = bbox_to_polygon(det.bbox).centroid
        return self._roi.contains(centroid)


class MinAreaFractionROIPostprocessor(BaseROIPostprocessor):

    def __init__(self, config: ROIConfigMinAreaFraction) -> None:
        self._config: ROIConfigMinAreaFraction = config
        super().__init__()

    @override
    def _build_roi(self) -> Polygon:
        return Polygon(self._config.coords)

    @override
    def _is_in_roi(self, det: RawDetection) -> bool:
        bbox_polygon: Polygon = bbox_to_polygon(det.bbox)
        if not bbox_polygon.area > 0:
            raise ValueError(f"Invalid bounding box, area must be greater than 0: {str(det.bbox.to_xyxy_list())}")
        intersection: BaseGeometry = shapely.intersection(
            self._roi, bbox_polygon
        )
        area_fraction_inside_roi: float = intersection.area / bbox_polygon.area

        return area_fraction_inside_roi >= self._config.min_area_fraction


def build_roi_postprocessor(config: ROIConfig) -> BaseROIPostprocessor:
    """
    A factory for `BaseROIFilterPostprocessor` based on `config`.
    """
    if isinstance(config, ROIConfigCentroid):
        return CentroidROIPostprocessor(config)
    elif isinstance(config, ROIConfigMinAreaFraction):
        return MinAreaFractionROIPostprocessor(config)
    raise RuntimeError(f"Unsupported ROI config type: {type(config)}")
