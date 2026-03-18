from typing import List, override, Any
from abc import ABC, abstractmethod
import logging
from logging import Logger
from pathlib import Path

from numpy import uint8
from numpy.typing import NDArray
from torch import Tensor
from ultralytics import YOLO
from ultralytics.engine.results import Boxes, Results

from common.settings.constants import ASSETS_DIR
from common.utils.fileops_utils import resolve_rel_path
from common.utils.logging_utils.logging_utils import get_logger_name_for_object
from tram_analytics.v1.models.common_types import BoundingBox
from tram_analytics.v1.models.components.detection import RawDetection
from tram_analytics.v1.pipeline.components.detection.detection_config import (
    DetectorConfig, DetectorConfigYOLO, DetectorConfigRemoteStub
)
from tram_analytics.v1.pipeline.components.detection.postprocessors import BaseROIPostprocessor, \
    build_roi_postprocessor


def convert_yolo_inference_result_to_rawdetections(boxes: Boxes) -> List[RawDetection]:
    """
    Convert the Boxes inference result for a single image
    into a list of RawDetection instances.
    """

    xyxy_src: Tensor | NDArray[Any] = boxes.xyxy # shape: (num_boxes, 4)
    conf_scores_src: Tensor | NDArray[Any] = boxes.conf # shape: (num_boxes,)
    class_ids_src: Tensor | NDArray[Any] = boxes.cls # shape: (num_boxes,)

    # [ [ box1_x1, ..., box1_y2 ], [box2_x1, ..., box2_y2], ... ]
    xyxy_coords_all: List[List[float]] = xyxy_src.tolist()
    conf_scores: List[float] = conf_scores_src.tolist()
    class_ids: List[float] = class_ids_src.tolist()

    detections: List[RawDetection] = [
        RawDetection(class_id=int(class_id),
                     confidence=conf_score,
                     bbox=BoundingBox(
                         x1=xyxy_coords[0],
                         y1=xyxy_coords[1],
                         x2=xyxy_coords[2],
                         y2=xyxy_coords[3]
                     ))
        for class_id, conf_score, xyxy_coords in zip(
            class_ids, conf_scores, xyxy_coords_all
        )
    ]

    return detections


class BaseDetector(ABC):

    """
    A single detection worker.
    """

    def __init__(self):
        self._logger: Logger = logging.getLogger(get_logger_name_for_object(self))
        self._roi_postprocessor: BaseROIPostprocessor | None = (
            self._get_roi_postprocessor()
        )

    @abstractmethod
    def _get_roi_postprocessor(self) -> BaseROIPostprocessor | None:
        pass

    @abstractmethod
    def start(self) -> None:
        pass

    @abstractmethod
    def stop(self) -> None:
        pass

    @abstractmethod
    def _get_all_detections(self, image_bgr: NDArray[uint8]) -> List[RawDetection]:
        """
        Feed the image to the detection worker and get all detections.
        """
        pass

    def detect(self, image_bgr: NDArray[uint8]) -> List[RawDetection]:
        self._logger.debug("Sending frame for inference")
        # run inference
        all_dets: List[RawDetection] = self._get_all_detections(image_bgr)
        self._logger.debug("Got raw detections")
        # filter the results
        filtered: List[RawDetection] = (
            self._roi_postprocessor.filter(all_dets)
            if self._roi_postprocessor is not None
            else all_dets
        )
        self._logger.debug("Returning inference results")
        return filtered


class YOLODetector(BaseDetector):

    """
    An in-process YOLO detection worker
    """

    def __init__(self, config: DetectorConfigYOLO):
        self._config: DetectorConfigYOLO = config
        self._model: YOLO | None = None
        super().__init__()

    @override
    def _get_roi_postprocessor(self) -> BaseROIPostprocessor | None:
        return (
            build_roi_postprocessor(self._config.roi) if self._config.roi is not None
            else None
        )

    @override
    def start(self) -> None:
        # idempotent: start only if not already started
        if self._model is None:
            weights_path: Path = resolve_rel_path(
                Path(self._config.weights_path), ASSETS_DIR
            )
            self._model = YOLO(weights_path,
                               **self._config.init_kwargs)

    @override
    def stop(self) -> None:
        self._model = None

    @override
    def _get_all_detections(self, image_bgr: NDArray[uint8]) -> List[RawDetection]:
        if self._model is None:
            raise ValueError("Can't run detect: model not initialised. "
                             "The instance must be started first.")
        logging.debug("Call to YOLO started")
        results: List[Results] = self._model(image_bgr,
                                             verbose=False,
                                             **self._config.run_kwargs)
        logging.debug("Call to YOLO returned")
        boxes: Boxes | None = results[0].boxes
        # for a detection model, boxes will not be null
        assert boxes is not None
        raw_detections: List[RawDetection] = convert_yolo_inference_result_to_rawdetections(boxes)
        return raw_detections


class DetectorRemoteStub(BaseDetector):

    def __init__(self, config: DetectorConfigRemoteStub):
        self._config: DetectorConfigRemoteStub = config
        super().__init__()

    @override
    def _get_roi_postprocessor(self) -> BaseROIPostprocessor | None:
        return (
            build_roi_postprocessor(self._config.roi) if self._config.roi is not None
            else None
        )

    @override
    def start(self) -> None:
        raise NotImplementedError()

    @override
    def stop(self) -> None:
        raise NotImplementedError()

    @override
    def _get_all_detections(self, image_bgr: NDArray[uint8]) -> List[RawDetection]:
        raise NotImplementedError()


def build_detector(config: DetectorConfig) -> BaseDetector:
    """
    A factory for `BaseDetector` based on `config`.
    """
    if isinstance(config, DetectorConfigYOLO):
        return YOLODetector(config)
    elif isinstance(config, DetectorConfigRemoteStub):
        return DetectorRemoteStub(config)
    raise RuntimeError(f"Unsupported detector config type: {type(config)}")
