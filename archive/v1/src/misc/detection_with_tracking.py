from typing import List

from PIL import ImageFile
from torch import Tensor
from ultralytics.engine.results import Boxes, Results

from archive.common.utils.img.img_bytes_conversion import pil_from_bytes_old
from tram_analytics.v1.pipeline.components.detection.detector import convert_yolo_inference_result_to_rawdetections, \
    YOLODetector
from tram_analytics.v1.models.components.detection import RawDetection


class RawDetectionWithTrack(RawDetection):
    track_id: int


def _convert_to_rawdetections_with_tracks(boxes: Boxes) -> List[RawDetectionWithTrack]:
    """
    Convert the Boxes inference result for a single image
    into a list of RawDetection instances.
    """

    dets_without_tracks: List[RawDetection] = convert_yolo_inference_result_to_rawdetections(boxes)
    track_id_tensor: Tensor | None = boxes.id # shape: (num_boxes,)
    if track_id_tensor is None:
        raise ValueError("The id property of the Boxes instance is None; tracks detection might not have been enabled.")
    tracks_ids: List[float] = track_id_tensor.tolist()
    dets_with_tracks: List[RawDetectionWithTrack] = [
        RawDetectionWithTrack(track_id=int(track_id),
                              **det.model_dump())
        for track_id, det in zip(tracks_ids, dets_without_tracks)
    ]
    return dets_with_tracks


class InProcessYOLODetectorWithTracking(YOLODetector):

    def detect_old(self, image: bytes) -> List[RawDetectionWithTrack]:
        if self._model is None:
            raise ValueError("Can't run detect: model not initialised. "
                             "The instance of InProcessDetector must be started first.")
        with pil_from_bytes_old(image) as img_pil: # type: ImageFile
            results: List[Results] = self._model.track(
                img_pil, persist=True, **self._model_run_kwargs
            )
            boxes: Boxes = results[0].boxes
            raw_detections: List[RawDetectionWithTrack] = _convert_to_rawdetections_with_tracks(boxes)
            return raw_detections
