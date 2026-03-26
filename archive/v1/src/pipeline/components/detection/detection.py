from multiprocessing import Queue
from typing import List

from PIL import ImageFile
from ultralytics import YOLO
from ultralytics.engine.results import Boxes

from archive.common.utils.img.img_bytes_conversion import pil_from_bytes_old
from tram_analytics.v1.models.components.detection import RawDetection
from tram_analytics.v1.pipeline.components.detection.components.detector import convert_yolo_inference_result_to_rawdetections


def _detection_worker(*, in_queue: Queue[bytes], out_queue: Queue[List[RawDetection]],
                      weights_path: str) -> None:
    # for offloading to a separate process
    model: YOLO = YOLO(weights_path)
    while True:
        image: bytes = in_queue.get()
        with pil_from_bytes_old(image) as img_pil: # type: ImageFile
            boxes: Boxes = model(img_pil, verbose=False)[0].boxes
            detections: List[RawDetection] = convert_yolo_inference_result_to_rawdetections(boxes)
            out_queue.put(detections)
