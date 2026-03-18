from typing import List, Tuple, Dict
from io import BytesIO
from datetime import datetime

import cv2
import numpy as np
from numpy.typing import NDArray
from PIL import ImageColor, Image, ImageFile
from PIL.ImageFile import ImageFile
from pydantic import BaseModel

from src.v1_1.models import Detection, Frame

from common.utils.img.cv2.drawing import draw_cross
from common.utils.img.cv2.pretty_put_text import pretty_put_text
from common.utils.img.img_bytes_conversion import bytes_from_pil
from archive.common.utils.img.img_bytes_conversion import pil_from_bytes_old
from common.utils.exec_timer import timed

LINE_TYPE: int = cv2.LINE_8
CROSS_SIZE: int = 9
CROSS_THICKNESS: int = 1
BBOX_THICKNESS: int = 1
BBOX_COLOR_BGR: Tuple[int, int, int] = (0, 0, 255)
ROI_COLOR_BGR: Tuple[int, int, int] = (57, 166, 170) # #aaa639 (yellow-green)

# { class_id: color_bgr }, color_bgr == (b, g, r)
CLASS_ID_TO_COLOR_MAP: Dict[int, Tuple[int, int, int]] = {
    # 0: (110, 38, 113) # #71266e (purple)
    0: (0, 0, 255),
    1: (255, 0, 0)
}

def color_bgr_from_rgb_string(rgb_string: str) -> Tuple[int, int, int]:
    color_rgb: Tuple[int, int, int] = ImageColor.getrgb(rgb_string)
    color_bgr: Tuple[int, int, int] = color_rgb[2], color_rgb[1], color_rgb[0]
    return color_bgr

def draw_polygon(img_bgr: NDArray[np.uint8],
                 *, vertices: List[List[int]],
                 thickness: int,
                 color_bgr: Tuple[int, int, int]) -> None:
    """
    Draws a polygon (unfilled) onto the image (in place).
    """
    cv2.polylines(img=img_bgr,
                  pts=vertices,
                  isClosed=True,
                  color=color_bgr,
                  thickness=thickness,
                  lineType=LINE_TYPE)

def draw_rectangle(img_bgr: NDArray[np.uint8],
                   pt1: Tuple[int, int],
                   pt2: Tuple[int, int],
                   color_bgr: Tuple[int, int, int],
                   thickness: int) -> None:
    """
    Draws a rectangle (unfilled) onto the image (in place).
    """
    cv2.rectangle(img=img_bgr,
                  pt1=pt1, pt2=pt2,
                  color=color_bgr,
                  thickness=thickness,
                  lineType=LINE_TYPE)


class TextConfig(BaseModel):
    # offset for the bottom-left corner of the text box, in pixels
    font_face: int = cv2.FONT_HERSHEY_SIMPLEX
    font_scale: float = 0.4
    thickness: int = 1
    line_type: int = cv2.LINE_8

class VisualizerConfig(BaseModel):
    _class_id_to_color_map: Dict[int, Tuple[int, int, int]] = CLASS_ID_TO_COLOR_MAP
    roi_color: Tuple[int, int, int] = ROI_COLOR_BGR
    bbox_thickness: int = 1

    # whether to draw on a greyscale version of the image (for better legibility)
    convert_to_gray: bool = True

    # whether to draw a cross at the bbox's centroid
    with_cross: bool = True
    cross_size: int = 9
    cross_thickness: int = 1

    text_config: TextConfig = TextConfig()

    # whether to draw confidence scores
    with_confidence: bool = True
    # whether to draw class IDs
    with_class_ids: bool = True

    def get_color_by_class_id(self, class_id: int) -> Tuple[int, int, int]:
        if class_id not in self._class_id_to_color_map:
            raise ValueError(f"Class ID {class_id} not found")
        return self._class_id_to_color_map[class_id]


class Visualizer:

    """
    Drawing methods for frames originating from a single camera.
    """

    def __init__(self, *,
                 roi: List[Tuple[int, int]] | None = None,
                 config: VisualizerConfig | None = None):
        self._roi: List[Tuple[int, int]] | None = roi
        self._config: VisualizerConfig = config if config is not None else VisualizerConfig()

    def _get_class_color(self, det: Detection):
        # wrapper for brevity
        return self._config.get_color_by_class_id(det.class_id)

    def _draw_roi(self, img_bgr: NDArray[np.uint8]):
        if self._roi is None:
            raise ValueError("Can't draw ROI: set to None.")
        cv2.polylines(img=img_bgr,
                      pts=self._roi,
                      isClosed=True,
                      color=self._config.roi_color,
                      thickness=self._config.bbox_thickness,
                      lineType=LINE_TYPE)

    def _draw_det_bbox(self, img_bgr: NDArray[np.uint8], det: Detection):
        cv2.rectangle(img=img_bgr,
                      pt1=(det.x1, det.y1), pt2=(det.x2, det.y2),
                      color=self._get_class_color(det),
                      thickness=self._config.bbox_thickness,
                      lineType=LINE_TYPE)

    @staticmethod
    def _get_detection_centroid(det: Detection) -> Tuple[int, int]:
        w: int = det.x2 - det.x1
        h: int = det.y2 - det.y1
        center_x: int = det.x1 + w // 2
        center_y: int = det.y1 + h // 2
        return center_x, center_y

    def _draw_det_centroid(self, img_bgr: NDArray[np.uint8], det: Detection):
        if not self._config.with_cross:
            raise ValueError(
                "Can't draw the centroid for the detection's bounding box with with_cross set to False"
            )
        cross_x, cross_y = self._get_detection_centroid(det)  # type: int, int
        draw_cross(img_bgr,
                   center=(cross_x, cross_y),
                   size=self._config.cross_size,
                   color=self._get_class_color(det),
                   thickness=self._config.cross_thickness)

    def _draw_det_confidence(self, img_bgr: NDArray[np.uint8], det: Detection):
        # .2f, offset (-5, -5) from top right corner
        text: str = f"{det.confidence:.2f}"
        pretty_put_text(img_bgr, text,
                        offset_from=(det.x2, det.y1),
                        offset=(-5, -5),
                        color=self._get_class_color(det),
                        **self._config.text_config.model_dump())

    def _draw_det_classid(self, img_bgr: NDArray[np.uint8], det: Detection):
        # .2f, offset (5, -5) from top left corner
        text: str = f"{det.class_id}"
        pretty_put_text(img_bgr, text,
                        offset_from=(det.x1, det.y1),
                        offset=(5, -5),
                        color=self._get_class_color(det),
                        **self._config.text_config.model_dump())


    def _draw_det(self, img_bgr: NDArray[np.uint8], det: Detection):
        self._draw_det_bbox(img_bgr, det)
        if self._config.with_cross:
            self._draw_det_centroid(img_bgr, det)
        if self._config.with_confidence:
            self._draw_det_confidence(img_bgr, det)
        if self._config.with_class_ids:
            self._draw_det_classid(img_bgr, det)

    def process_frame(self, frame: Frame, dets: List[Detection]) -> bytes:
        with timed("bytes -> pil img -> ndarray rgb -> ndarray bgr"):
            with pil_from_bytes_old(frame.image) as pil_img: # type: ImageFile
                img_format: str = pil_img.format
                src_img_rgb: NDArray[np.uint8] = np.array(pil_img)
            img_bgr: NDArray[np.uint8] = cv2.cvtColor(src=src_img_rgb, code=cv2.COLOR_RGB2BGR)
            if self._config.convert_to_gray:
                # to 1-dim greyscale
                img_bgr = cv2.cvtColor(src=img_bgr, code=cv2.COLOR_BGR2GRAY)
                # back to 3-dim BGR (but now a greyscale image)
                img_bgr = cv2.cvtColor(src=img_bgr, code=cv2.COLOR_GRAY2BGR)

        with timed("drawing"):
            # draw detections
            for det in dets: # type: Detection
                self._draw_det(img_bgr, det)

        with timed("ndarray bgr -> ndarray rgb -> pil img -> bytes"):
            src_img_rgb = cv2.cvtColor(src=img_bgr, code=cv2.COLOR_BGR2RGB)
            with Image.fromarray(src_img_rgb) as pil_img: # type: ImageFile, BytesIO
                dest_img: bytes = bytes_from_pil(pil_img, img_format=img_format)

        return dest_img

def _sandbox_1():
    from typing import Dict, Any

    img_size: int = 100
    color_rgb: str = "#ff0000"
    thickness: int = 2
    cross_params: Dict[str, Any] = {
        "center": (30, 40),
        "size": 15
    }

    img: NDArray[np.uint8] = np.full(shape=(img_size, img_size, 3), fill_value=255,
                                     dtype=np.uint8)
    color_bgr: Tuple[int, int, int] = color_bgr_from_rgb_string(color_rgb)
    draw_cross(img=img, thickness=thickness, color=color_bgr, **cross_params)
    cv2.imshow("image_1", img)
    cv2.waitKey(0)

def _sandbox_2():
    from common.utils.img.img_bytes_conversion import write_img_bytes_to_path

    src_path: str = "REDACTED/dataset/selected_320px/val/20251107_190352.jpg"
    dest_path: str = "REDACTED/20251107_190352.png"
    frame_id: str = "frame_a98cf"
    camera_id: str = "cam_1"
    timestamp: datetime = datetime.fromisoformat("2025-12-07T10:02:17.420+00:00")

    with Image.open(src_path) as pil_img, BytesIO() as stream: # type: ImageFile
        pil_img.save(fp=stream, format="PNG")
        img: bytes = stream.getvalue()

    frame: Frame = Frame(frame_id=frame_id,
                         camera_id=camera_id,
                         image=img,
                         timestamp=timestamp)
    det_1: Detection = Detection(detection_id="det_176",
                                 frame_id=frame_id,
                                 class_id=0,
                                 confidence=0.874621,
                                 x1=200, x2=260, y1=100, y2=130)
    det_2: Detection = Detection(detection_id="det_137",
                                 frame_id=frame_id,
                                 class_id=1,
                                 confidence=0.927390,
                                 x1=225, x2=295, y1=110, y2=145)

    visualizer: Visualizer = Visualizer()
    dest_img: bytes = visualizer.process_frame(frame, [det_1, det_2])
    write_img_bytes_to_path(dest_img, dest_path, img_format="PNG")


if __name__ == "__main__":
    _sandbox_2()