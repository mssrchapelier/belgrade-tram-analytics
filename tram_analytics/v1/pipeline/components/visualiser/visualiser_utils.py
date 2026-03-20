from typing import List, Tuple, Any, cast
from dataclasses import dataclass
from itertools import pairwise

import cv2
from numpy import uint8, int64
from numpy.typing import NDArray

from common.utils.custom_types import PixelPosition
from common.utils.img.cv2.drawing import dashed_line
from tram_analytics.v1.pipeline.components.visualiser.settings import (
    LINE_TYPE, CORRIDOR_DASH_LENGTH, CORRIDOR_GAP_LENGTH, CORRIDOR_COLOUR, CORRIDOR_THICKNESS
)

def bgr_render_as_grey(img_bgr: NDArray[uint8]) -> NDArray[uint8]:
    # to 1-dim greyscale
    grey: NDArray[uint8] = cv2.cvtColor(src=img_bgr, code=cv2.COLOR_BGR2GRAY).astype(uint8, copy=False)
    # back to 3-dim BGR (but now a greyscale image)
    grey = cv2.cvtColor(src=grey, code=cv2.COLOR_GRAY2BGR).astype(uint8, copy=False)
    return grey


@dataclass(slots=True, kw_only=True)
class RailTrackNumpy:
    # shape: (num_vertices, 2), dtype: int64
    polygon: NDArray[int64]
    # shape: (num_vertices, 2), dtype: int64
    centreline: NDArray[int64]


def draw_rail_track(img: NDArray[uint8], corridor: RailTrackNumpy) -> None:
    # border
    border_vertices: List[PixelPosition] = [
        (vertex_row[0], vertex_row[1])
        for vertex_row in corridor.polygon.tolist()
    ]
    # [ (xy1, xy2), (xy2, xy3), ... ]
    border_vertex_pairs: List[Tuple[PixelPosition, PixelPosition]] = list(pairwise(border_vertices))
    # also connect the last vertex with the first one
    border_vertex_pairs.append(
        (border_vertices[-1], border_vertices[0])
    )
    # track border
    for border_segment_start, border_segment_end in border_vertex_pairs: # type: PixelPosition, PixelPosition
        dashed_line(img, border_segment_start, border_segment_end,
                    dash=CORRIDOR_DASH_LENGTH, gap=CORRIDOR_GAP_LENGTH,
                    color=CORRIDOR_COLOUR, thickness=CORRIDOR_THICKNESS,
                    lineType=LINE_TYPE)
    # centre line
    centreline_pts: List[PixelPosition] = [
        (pt_row[0], pt_row[1])
        for pt_row in corridor.centreline.tolist()
    ]
    for centreline_segment_start, centreline_segment_end in pairwise(centreline_pts): # type: PixelPosition, PixelPosition
        cv2.line(img=img, pt1=centreline_segment_start, pt2=centreline_segment_end,
                 color=CORRIDOR_COLOUR, thickness=CORRIDOR_THICKNESS,
                 lineType=LINE_TYPE)
