from typing import Tuple, Dict, Any, List
from itertools import pairwise

import cv2
import numpy as np
from numpy.typing import NDArray, DTypeLike


def dashed_line(img: NDArray,
                pt1: Tuple[int, int], pt2: Tuple[int, int],
                *, dash: int, gap: int,
                color: Tuple[int, int, int],
                thickness: int,
                lineType: int) -> None:
    pt1_arr: NDArray = np.array(pt1, dtype=np.float32)
    pt2_arr: NDArray = np.array(pt2, dtype=np.float32)
    diff: NDArray = pt2_arr - pt1_arr
    length: float = float(np.linalg.norm(diff))
    if length == 0:
        return
    direction: NDArray = diff / length

    dist_for_cur_start: float = 0.0
    while dist_for_cur_start < length:
        dash_start: NDArray = (
            pt1_arr + direction * dist_for_cur_start
        ).round().astype(np.int32)
        dash_end: NDArray = (
            pt1_arr + direction * min(dist_for_cur_start + dash, length)
        ).round().astype(np.int32)
        cv2.line(img=img, pt1=dash_start, pt2=dash_end,
                 color=color, thickness=thickness, lineType=lineType)
        dist_for_cur_start += dash + gap


def dashed_rectangle(img: NDArray,
                     pt1: Tuple[int, int], pt2: Tuple[int, int],
                     **kwargs) -> None:
    x1, y1 = pt1 # type: int, int
    x2, y2 = pt2 # type: int, int
    vertices: List[Tuple[int, int]] = [
        (x1, y1), (x2, y1), (x2, y2), (x1, y2), (x1, y1)
    ]
    for v1, v2 in pairwise(vertices): # type: Tuple[int, int], Tuple[int, int]
        dashed_line(img, v1, v2, **kwargs)


def draw_cross(img: NDArray[np.uint8],
               *, center: Tuple[int, int],
               size: int,
               color: Tuple[int, int, int],
               thickness: int,
               lineType: int):
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
    cv2.line(img=img, pt1=hor_line_pt1, pt2=hor_line_pt2,
             color=color, thickness=thickness, lineType=lineType)
    # draw the vertical line
    cv2.line(img=img, pt1=vert_line_pt1, pt2=vert_line_pt2,
             color=color, thickness=thickness, lineType=lineType)
