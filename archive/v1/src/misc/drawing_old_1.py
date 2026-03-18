from typing import List, Tuple, Dict
from io import BytesIO
from datetime import datetime

import cv2
import numpy as np
from numpy.typing import NDArray
from PIL import ImageColor, Image, ImageFile
from PIL.ImageFile import ImageFile
from pydantic import BaseModel

from common.utils.custom_types import ColorTuple
from archive.v1.src.models.models import (
    PixelBoundingBox, TrackState_Old, FrameWithBytes
)
from tram_analytics.v1.models.common_types import BoundingBox
from tram_analytics.v1.models.components.detection import Detection
from common.utils.img.cv2.pretty_put_text import pretty_put_text
from common.utils.img.img_bytes_conversion import bytes_from_pil
from archive.common.utils.img.img_bytes_conversion import pil_from_bytes_old
from common.utils.exec_timer import timed
from common.utils.img.cv2.drawing import dashed_rectangle

LINE_TYPE: int = cv2.LINE_8
CROSS_SIZE: int = 9
CROSS_THICKNESS: int = 1
BBOX_THICKNESS: int = 1
BBOX_COLOR_BGR: ColorTuple = (0, 0, 255)
ROI_COLOR_BGR: ColorTuple = (57, 166, 170) # #aaa639 (yellow-green)

# { class_id: color_bgr }, color_bgr == (b, g, r)
CLASS_ID_TO_COLOR_MAP: Dict[int, ColorTuple] = {
    # 0: (110, 38, 113) # #71266e (purple)
    0: (0, 0, 255),
    1: (255, 0, 0)
}

def color_bgr_from_rgb_string(rgb_string: str) -> ColorTuple:
    color_rgb: ColorTuple = ImageColor.getrgb(rgb_string)
    color_bgr: ColorTuple = color_rgb[2], color_rgb[1], color_rgb[0]
    return color_bgr

def draw_polygon(img_bgr: NDArray[np.uint8],
                 *, vertices: List[List[int]],
                 thickness: int,
                 color_bgr: ColorTuple) -> None:
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
                   color_bgr: ColorTuple,
                   thickness: int) -> None:
    """
    Draws a rectangle (unfilled) onto the image (in place).
    """
    cv2.rectangle(img=img_bgr,
                  pt1=pt1, pt2=pt2,
                  color=color_bgr,
                  thickness=thickness,
                  lineType=LINE_TYPE)

def draw_cross(img_bgr: NDArray[np.uint8],
               *, center: Tuple[int, int],
               size: int,
               color_bgr: ColorTuple,
               thickness: int):
    """
    Draw a cross of width and height `size` centred on `center`.
    """
    if size % 2 != 1:
        raise ValueError(f"size must be an odd number, got: {size}")

    hor_line_y: int = center[1]
    hor_line_x1: int = center[0] - size // 2
    hor_line_x2: int = center[0] + size // 2
    hor_line_pt1: Tuple[int, int] = (hor_line_x1, hor_line_y)
    hor_line_pt2: Tuple[int, int] = (hor_line_x2, hor_line_y)

    vert_line_x: int = center[0]
    vert_line_y1: int = center[1] - size // 2
    vert_line_y2: int = center[1] + size // 2
    vert_line_pt1: Tuple[int, int] = (vert_line_x, vert_line_y1)
    vert_line_pt2: Tuple[int, int] = (vert_line_x, vert_line_y2)

    # draw the horizontal line
    cv2.line(img=img_bgr, pt1=hor_line_pt1, pt2=hor_line_pt2,
             color=color_bgr, thickness=thickness, lineType=LINE_TYPE)
    # draw the vertical line
    cv2.line(img=img_bgr, pt1=vert_line_pt1, pt2=vert_line_pt2,
             color=color_bgr, thickness=thickness, lineType=LINE_TYPE)

class TextConfig(BaseModel):
    # offset for the bottom-left corner of the text box, in pixels
    font_face: int = cv2.FONT_HERSHEY_SIMPLEX
    font_scale: float = 0.4
    thickness: int = 1
    line_type: int = cv2.LINE_8

class DetClassColorConfig(BaseModel):
    # detection bbox border
    detection: ColorTuple
    # tracker-predicted bbox border
    tracker_bbox: ColorTuple


class ColorConfig(BaseModel):
    roi: ColorTuple = ROI_COLOR_BGR
    classes: Dict[int, DetClassColorConfig]


class VisualizerConfig(BaseModel):
    colors: ColorConfig

    bbox_thickness: int = 1

    # whether to draw on a greyscale version of the image (for better legibility)
    convert_to_gray: bool = True

    # whether to draw a cross at the bbox's centroid
    with_cross: bool = True
    cross_size: int = 9
    cross_thickness: int = 1

    # for dashed lines
    dash_length: int = 5
    dash_gap: int = 5

    text_config: TextConfig = TextConfig()

    # whether to draw confidence scores
    with_confidence: bool = True
    # whether to draw class IDs
    with_class_ids: bool = True

    def get_color_by_class_id(self, class_id: int) -> ColorTuple:
        if class_id not in self._class_id_to_color_map:
            raise ValueError(f"Class ID {class_id} not found")
        return self._class_id_to_color_map[class_id]


class Visualizer:

    """
    Drawing methods for frames originating from a single camera.
    """

    def __init__(self, *,
                 roi: List[Tuple[int, int]] | None = None,
                 config: VisualizerConfig):
        self._roi: List[Tuple[int, int]] | None = roi
        self._config: VisualizerConfig = config

    def _draw_roi(self, img_bgr: NDArray[np.uint8]):
        if self._roi is None:
            raise ValueError("Can't draw ROI: set to None.")
        cv2.polylines(img=img_bgr,
                      pts=self._roi,
                      isClosed=True,
                      color=self._config.colors.roi,
                      thickness=self._config.bbox_thickness,
                      lineType=LINE_TYPE)

    def _draw_bbox(self, img_bgr: NDArray[np.uint8], bbox: BoundingBox,
                   *, color: ColorTuple, dashed: bool = False):
        px_bbox: PixelBoundingBox = PixelBoundingBox.from_float_bbox(bbox)
        pt1: Tuple[int, int] = (px_bbox.x1, px_bbox.y1)
        pt2: Tuple[int, int] = (px_bbox.x2, px_bbox.y2)
        if dashed:
            dashed_rectangle(img=img_bgr,
                             pt1=pt1, pt2=pt2,
                             dash=self._config.dash_length,
                             gap=self._config.dash_gap,
                             color=color,
                             thickness=self._config.bbox_thickness,
                             lineType=LINE_TYPE)
        else:
            cv2.rectangle(img=img_bgr,
                          pt1=pt1, pt2=pt2,
                          color=color,
                          thickness=self._config.bbox_thickness,
                          lineType=LINE_TYPE)

    @staticmethod
    def _get_bbox_centroid(bbox: BoundingBox) -> Tuple[float, float]:
        w: float = bbox.x2 - bbox.x1
        h: float = bbox.y2 - bbox.y1
        center_x: float = bbox.x1 + w / 2
        center_y: float = bbox.y1 + h / 2
        return center_x, center_y

    def _draw_bbox_centroid(self, img_bgr: NDArray[np.uint8], bbox: BoundingBox,
                            *, color: ColorTuple):
        if not self._config.with_cross:
            raise ValueError(
                "Can't draw the centroid for the detection's bounding box with with_cross set to False"
            )
        cross_x, cross_y = self._get_bbox_centroid(bbox)  # type: float, float
        draw_cross(img_bgr,
                   center=(round(cross_x), round(cross_y)),
                   size=self._config.cross_size,
                   color_bgr=color,
                   thickness=self._config.cross_thickness)

    def _draw_det_confidence(self, img_bgr: NDArray[np.uint8], det: Detection):
        # .2f, offset (-5, -5) from top right corner
        px_bbox: PixelBoundingBox = PixelBoundingBox.from_float_bbox(det.raw_detection.bbox)
        text: str = f"{det.raw_detection.confidence:.2f}"
        pretty_put_text(img_bgr, text,
                        offset_from=(px_bbox.x2, px_bbox.y1),
                        offset=(-5, -5),
                        color=self._config.colors.classes[det.raw_detection.class_id].detection,
                        **self._config.text_config.model_dump())

    def _draw_bbox_classid(self, img_bgr: NDArray[np.uint8], bbox: BoundingBox, *, class_id: int,
                           color: ColorTuple):
        # .2f, offset (5, -5) from top left corner
        px_bbox: PixelBoundingBox = PixelBoundingBox.from_float_bbox(bbox)
        text: str = f"{class_id}"
        pretty_put_text(img_bgr, text,
                        offset_from=(px_bbox.x1, px_bbox.y1),
                        offset=(5, -5),
                        color=color,
                        **self._config.text_config.model_dump())


    def _draw_det(self, img_bgr: NDArray[np.uint8], det: Detection):
        bbox: BoundingBox = det.raw_detection.bbox
        class_id: int = det.raw_detection.class_id
        color: ColorTuple = self._config.colors.classes[class_id].detection
        self._draw_bbox(img_bgr, bbox, color=color)
        if self._config.with_cross:
            self._draw_bbox_centroid(img_bgr, bbox, color=color)
        if self._config.with_confidence:
            self._draw_det_confidence(img_bgr, det)
        if self._config.with_class_ids:
            self._draw_bbox_classid(img_bgr, bbox, class_id=class_id, color=color)

    def _draw_track_state(self, img_bgr: NDArray[np.uint8], track_state: TrackState_Old,
                          *, class_id: int):
        bbox: BoundingBox = track_state.bbox
        color: ColorTuple = self._config.colors.classes[class_id].tracker_bbox
        self._draw_bbox(img_bgr, bbox, color=color, dashed=True)
        if self._config.with_cross:
            self._draw_bbox_centroid(img_bgr, bbox, color=color)

    def process_frame(self, frame: FrameWithBytes,
                      dets: List[Detection],
                      track_states: List[TrackState_Old]) -> bytes:
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
            for state in track_states: # type: TrackState_Old
                self._draw_track_state(img_bgr, state,
                                       # set class ID to 0 for now
                                       class_id=0)

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
    draw_cross(img_bgr=img, thickness=thickness, color_bgr=color_bgr, **cross_params)
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

    frame: FrameWithBytes = FrameWithBytes(frame_id=frame_id,
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