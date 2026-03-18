from typing import List, Tuple, Dict, Any
from datetime import datetime, timedelta

from pydantic import BaseModel
import numpy as np
from numpy.typing import NDArray
import cv2

from tram_analytics.v1.models.common_types import BoundingBox
from common.utils.custom_types import ColorTuple
from common.utils.img.cv2.drawing import dashed_rectangle

LINE_TYPE: int = cv2.LINE_8

# matched unconfirmed: dashed
MATCHED_UNCONFIRMED_DASH_LENGTH: int = 5
MATCHED_UNCONFIRMED_DASH_GAP: int = 10
# unmatched: dotted
UNMATCHED_DASH_LENGTH: int = 2
UNMATCHED_DASH_GAP: int = 10

BBOX_COLOR: ColorTuple = (0, 0, 255)
BBOX_THICKNESS: int = 2


class TrackStateForDrawing(BaseModel):
    timestamp: datetime
    is_matched: bool
    is_confirmed: bool
    bbox: BoundingBox

class TrackForDrawing(BaseModel):
    track_id: str
    class_id: int
    history: List[TrackStateForDrawing]

def xyxy_to_centroids(xyxy: NDArray) -> NDArray:
    # input shape: (num_bboxes, 4)
    # output shape: (num_bboxes, 2); column 0: x_center; column 1: y_center
    if not (len(xyxy.shape) == 2 and xyxy.shape[1] == 4):
        raise ValueError(f"xyxy must be of shape (num_bboxes, 2), got: {str(xyxy.shape)}")
    x1, y1, x2, y2 = np.split(xyxy, 4, axis=1) # shape of each: (num_bboxes, 1)
    x_center: NDArray = x1 + (x2 - x1) / 2.0
    y_center: NDArray = y1 + (y2 - y1) / 2.0
    centroids: NDArray = np.concatenate([x_center, y_center], axis=1) # shape: (num_bboxes, 2)
    return centroids


def draw_track(img: NDArray, history: List[TrackStateForDrawing],
               *, color: ColorTuple, thickness: int):
    """
    Draw a polyline connecting the centroids of the track's successive states.
    """
    xyxy: NDArray = np.array([state.bbox.to_xyxy_list() for state in history],
                             dtype=np.float32) # shape: (num_states, 4)
    centroids: NDArray = xyxy_to_centroids(xyxy).round().astype(np.int32) # shape: (num_states, 2)
    # polylines: NDArray = centroids.reshape(-1, 1, 2)
    cv2.polylines(img=img, pts=[centroids], isClosed=False, color=color,
                  thickness=thickness, lineType=LINE_TYPE)

def draw_track_bbox(img: NDArray, state: TrackStateForDrawing):
    # matched, not confirmed: dashed
    # matched, confirmed: solid
    # unmatched: dotted
    if not state.is_matched and not state.is_confirmed:
        raise ValueError("At least one of is_matched, is_confirmed must be True for a track state")
    # xyxy: NDArray = np.array(state.bbox.to_xyxy_list()).round().astype(dtype=np.int32)
    # pt1: NDArray = xyxy[0:2]
    # pt2: NDArray = xyxy[2:]
    pt1: Tuple[int, int] = (round(state.bbox.x1), round(state.bbox.y1))
    pt2: Tuple[int, int] = (round(state.bbox.x2), round(state.bbox.y2))
    kwargs: Dict[str, Any] = {
        "img": img, "pt1": pt1, "pt2": pt2, "color": BBOX_COLOR,
        "thickness": BBOX_THICKNESS, "lineType": LINE_TYPE
    }
    if state.is_matched and state.is_confirmed:
        cv2.rectangle(**kwargs)
    elif state.is_matched and not state.is_confirmed:
        # dashed
        dashed_rectangle(dash=MATCHED_UNCONFIRMED_DASH_LENGTH, gap=MATCHED_UNCONFIRMED_DASH_GAP, **kwargs)
    else:
        # dotted
        dashed_rectangle(dash=UNMATCHED_DASH_LENGTH, gap=UNMATCHED_DASH_GAP, **kwargs)

def _draw_track_state_info(self, img: NDArray, state: TrackStateForDrawing):
    # .2f, offset (-5, -5) from top right corner
    px_bbox: PixelBoundingBox = PixelBoundingBox.from_float_bbox(det.raw_detection.bbox)
    text: str = f"{det.raw_detection.confidence:.2f}"
    pretty_put_text(img_bgr, text,
                    offset_from=(px_bbox.x2, px_bbox.y1),
                    offset=(-5, -5),
                    color=self._config.colors.classes[det.raw_detection.class_id].detection,
                    **self._config.text_config.model_dump())

# ---

def _get_simulated_track():
    bbox_w, bbox_h = 50.0, 70.0

    def _get_bbox(centroid: Tuple[float, float]) -> BoundingBox:
        x_c, y_c = centroid
        x1 = x_c - bbox_w / 2.0
        x2 = x_c + bbox_w / 2.0
        y1 = y_c - bbox_h / 2.0
        y2 = y_c + bbox_h / 2.0
        return BoundingBox(x1=x1, x2=x2, y1=y1, y2=y2)

    # centroids
    history: List[Tuple[float, float]] = [
        (100.0, 400.0), (130.0, 420.0), (170.0, 420.0),
        (170.0, 470.0), (210.0, 510.0), (230.0, 515.0)
    ]
    bboxes: List[BoundingBox] = [_get_bbox(centroid) for centroid in history]
    is_matched: List[bool] = [True, True, True, False, True, True]
    is_confirmed: List[bool] = [False, False, True, True, True, True]
    timestamps: List[datetime] = [
        datetime.fromisoformat("2025-10-16T14:00:00+00:00") + timedelta(seconds=i)
        for i in range(len(history))
    ]

    states: List[TrackStateForDrawing] = [
        TrackStateForDrawing(timestamp=ts, is_matched=m, is_confirmed=c, bbox=bbox)
        for ts, m, c, bbox in zip(timestamps, is_matched, is_confirmed, bboxes)
    ]
    return states



def _sandbox():
    history: List[TrackStateForDrawing] = _get_simulated_track()
    canvas: NDArray = np.full(shape=(700, 1200, 3), fill_value=255, dtype=np.uint8)
    draw_track(canvas, history, color=(255, 0, 0), thickness=BBOX_THICKNESS)
    for state in history: # type: TrackStateForDrawing
        draw_track_bbox(canvas, state)
    out_path: str = "REDACTED/track_1.png"
    cv2.imwrite(out_path, canvas)
    print("done")

def _sandbox_1():
    print(type(2 / 3))

if __name__ == "__main__":
    _sandbox_1()