from typing import List, Self, Dict, Any
from multiprocessing.queues import Queue
from abc import ABC, abstractmethod

from pydantic import BaseModel, Field
from ultralytics import YOLO
from ultralytics.engine.results import Results, Boxes
from PIL import ImageFile
from torch import Tensor
from shapely import Polygon

from archive.common.utils.img.img_bytes_conversion import pil_from_bytes_old
from src.v1_1.models import Frame, Detection
from src.v1_1.utils import get_uuid

class RawDetection(BaseModel):
    class_id: int
    confidence: float = Field(ge=0.0, le=1.0)
    track_id: int
    # x1, x2, y1, y2: absolute values (pixels)
    x1: int = Field(ge=1)
    x2: int = Field(ge=1)
    y1: int = Field(ge=1)
    y2: int = Field(ge=1)


def _convert_to_rawdetections(boxes: Boxes) -> List[RawDetection]:
    """
    Convert the Boxes inference result for a single image
    into a list of RawDetection instances.
    """

    xyxy_tensor: Tensor = boxes.xyxy # shape: (num_boxes, 4)
    conf_tensor: Tensor = boxes.conf # shape: (num_boxes,)
    class_id_tensor: Tensor = boxes.cls # shape: (num_boxes,)
    track_id_tensor: Tensor | None = boxes.id  # shape: (num_boxes,)

    # [ [ box1_x1, ..., box1_y2 ], [box2_x1, ..., box2_y2], ... ]
    xyxy_coords_all: List[List[float]] = xyxy_tensor.tolist()
    conf_scores: List[float] = conf_tensor.tolist()
    class_ids: List[float] = class_id_tensor.tolist()
    track_ids: List[float] = track_id_tensor.tolist()

    detections: List[RawDetection] = [
        RawDetection(class_id=int(class_id),
                     track_id=int(track_id),
                     confidence=conf_score,
                     x1=round(xyxy_coords[0]),
                     y1=round(xyxy_coords[1]),
                     x2=round(xyxy_coords[2]),
                     y2=round(xyxy_coords[3]))
        for class_id, track_id, conf_score, xyxy_coords in zip(
            class_ids, track_ids, conf_scores, xyxy_coords_all
        )
    ]

    return detections


def _detection_worker(*, in_queue: Queue[bytes], out_queue: Queue[List[RawDetection]],
                      weights_path: str) -> None:
    # for offloading to a separate process
    model: YOLO = YOLO(weights_path)
    while True:
        image: bytes = in_queue.get()
        with pil_from_bytes_old(image) as img_pil: # type: ImageFile
            boxes: Boxes = model(img_pil)[0].boxes
            detections: List[RawDetection] = _convert_to_rawdetections(boxes)
            out_queue.put(detections)

class DetectionPostprocessor(ABC):

    @abstractmethod
    def process(self, detections: List[RawDetection]) -> List[RawDetection]:
        pass

def detection_to_polygon(det: RawDetection) -> Polygon:
    polygon: Polygon = Polygon(
        [(det.x1, det.y1), (det.x2, det.y1),
         (det.x2, det.y2), (det.x1, det.y2)]
    )
    return polygon

class ROIFilterPostprocessor(DetectionPostprocessor):

    def __init__(self,
                 roi_coords: List[List[float]]):
        # roi_coords: [ [x_1, y_1], ..., [x_n, y_n] ] for each of the n vertices defining the ROI
        self._roi: Polygon = Polygon(roi_coords)

    def process(self, detections: List[RawDetection]) -> List[RawDetection]:
        """
        Remove boxes whose centroids lie outside the ROI.
        """
        filtered: List[RawDetection] = list(filter(
            lambda det: self._roi.contains(
                detection_to_polygon(det).centroid
            ),
            detections
        ))
        return filtered

# ... postprocessing logic ...

class BaseDetector(ABC):

    """
    A single detection worker.
    """

    def __init__(self, detector_id: str):
        self._detector_id: str = detector_id

    @abstractmethod
    def start(self) -> Self:
        pass

    @abstractmethod
    def stop(self) -> None:
        pass

    @abstractmethod
    def detect(self, image: bytes) -> List[RawDetection]:
        pass

class InProcessYOLODetector(BaseDetector):

    """
    An in-process YOLO detection worker
    """

    def __init__(self, *, detector_id: str, weights_path: str,
                 model_init_kwargs: Dict[str, Any] | None = None,
                 model_run_kwargs: Dict[str, Any] | None = None):
        super().__init__(detector_id)

        self._weights_path: str = weights_path
        self._model_init_kwargs: Dict[str, Any] = (model_init_kwargs
                                                   if model_init_kwargs is not None
                                                   else dict())
        self._model_run_kwargs: Dict[str, Any] = (model_run_kwargs
                                                  if model_run_kwargs is not None
                                                  else dict())

        self._model: YOLO | None = None

    def start(self) -> Self:
        # idempotent: start only if not already started
        if self._model is None:
            self._model = YOLO(self._weights_path, **self._model_init_kwargs)

    def stop(self) -> None:
        self._model = None

    def detect(self, image: bytes) -> List[RawDetection]:
        if self._model is None:
            raise ValueError("Can't run detect: model not initialised. "
                             "The instance of InProcessDetector must be started first.")
        with pil_from_bytes_old(image) as img_pil: # type: ImageFile
            results: List[Results] = self._model.track(
                img_pil, persist=True, **self._model_run_kwargs
            )
            boxes: Boxes = results[0].boxes
            raw_detections: List[RawDetection] = _convert_to_rawdetections(boxes)
            return raw_detections


class DetectionService:

    """
    Wraps multiple detection workers.
    Should be used as a resource.
    """

    def __init__(self, workers: List[BaseDetector]):
        self._workers: List[BaseDetector] = workers

    def __enter__(self) -> Self:
        for worker in self._workers: # type: BaseDetector
            worker.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        for worker in self._workers: # type: BaseDetector
            worker.stop()

    def detect(self, frame: Frame) -> List[Detection]:
        dets: List[Detection] = []
        for worker in self._workers: # type: BaseDetector
            raw_dets: List[RawDetection] = worker.detect(frame.image)
            for raw_det in raw_dets: # type: RawDetection
                det_id: str = get_uuid()
                det: Detection = Detection(frame_id=frame.frame_id,
                                           detection_id=det_id,
                                           **raw_det.model_dump())
                dets.append(det)
        return dets
