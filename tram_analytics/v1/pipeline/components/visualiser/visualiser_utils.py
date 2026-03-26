from typing import List, Tuple
from dataclasses import dataclass
from itertools import pairwise

import cv2
from numpy import uint8, int64
from numpy.typing import NDArray

from common.utils.custom_types import PixelPosition, ColorTuple
from common.utils.img.cv2.drawing import dashed_line, to_px, dashed_rectangle
from common.utils.img.cv2.pretty_put_text import anchor_line_with_bg
from tram_analytics.v1.models.common_types import BoundingBox, convert_speed, get_speed_unit_str, VehicleType
from tram_analytics.v1.pipeline.components.visualiser.config.colour_palette import (
    TrackLineMarkerColorPalette, LineColorPaletteItem
)
from tram_analytics.v1.pipeline.components.visualiser.config.visualiser_config import (
    SpeedConfig, ColorlessTextboxConfig, TrackStateLineAppearanceConfig,
    DashedLineConfig, ClassIDConfig, TrackIDConfig
)
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


def _draw_vehicle_speed(
        img: NDArray[uint8], speed: float | None,
        *, px_bbox: BoundingBox,
        config: SpeedConfig,
        bg_color: ColorTuple,
        text_color: ColorTuple
) -> None:
    speed_converted: float | None = convert_speed(speed, config.unit)
    value_text: str = str(round(speed_converted)) if speed_converted is not None else "N/A"
    num_digits: int = config.render.display_length
    unit_str: str = get_speed_unit_str(config.unit)
    # truncate or pad with spaces if shorter; align right
    # examples: "  5 km/h", " 12 m/s"
    formatted: str = f"{value_text[:num_digits]: >{num_digits}} {unit_str}"
    textbox_config: ColorlessTextboxConfig = config.render.textbox
    # position: inside bbox, top left corner
    anchor: PixelPosition = to_px((px_bbox.x1, px_bbox.y1))
    anchor_line_with_bg(img, formatted,
                        anchor=anchor,
                        which="tl",
                        offset=textbox_config.offset,
                        padding=textbox_config.padding,
                        bg_color=bg_color,
                        font_color=text_color,
                        font_face=textbox_config.font_face,
                        font_scale=textbox_config.font_scale,
                        thickness=textbox_config.thickness,
                        line_type=LINE_TYPE)


def _get_track_line_marker_palette_item(palette: TrackLineMarkerColorPalette,
                                        *, is_confirmed: bool, is_matched: bool) -> LineColorPaletteItem:
    if is_confirmed:
        if is_matched:
            return palette.confirmed_matched
        return palette.confirmed_unmatched
    else:
        if is_matched:
            return palette.unconfirmed_matched
        return palette.unconfirmed_unmatched


def _get_track_line_color(palette: TrackLineMarkerColorPalette,
                          *, is_confirmed: bool, is_matched: bool) -> ColorTuple:
    palette_item: LineColorPaletteItem = _get_track_line_marker_palette_item(
        palette, is_confirmed=is_confirmed, is_matched=is_matched
    )
    return palette_item.line


def _get_track_marker_color(palette: TrackLineMarkerColorPalette,
                            *, is_confirmed: bool, is_matched: bool) -> ColorTuple:
    palette_item: LineColorPaletteItem = _get_track_line_marker_palette_item(
        palette, is_confirmed=is_confirmed, is_matched=is_matched
    )
    return palette_item.marker


def _draw_track_segment(img: NDArray[uint8], start: PixelPosition, end: PixelPosition,
                        *, color: ColorTuple, thickness: int):
    cv2.line(img=img, pt1=start, pt2=end, color=color,
             thickness=thickness, lineType=LINE_TYPE)


def _draw_marker(img: NDArray[uint8], center: PixelPosition,
                 *, marker_size: int, color: ColorTuple):
    if marker_size <= 0 or marker_size % 2 == 0:
        raise ValueError(f"marker_size must be an odd positive integer, received: {marker_size}")

    center_x, center_y = center # type: int, int
    half_size: int = marker_size // 2
    x1: int = center_x - half_size
    x2: int = center_x + half_size
    y1: int = center_y - half_size
    y2: int = center_y + half_size
    cv2.rectangle(img=img, pt1=(x1, y1), pt2=(x2, y2), color=color, thickness=-1)


def _get_dashed_line_config(parent_config: TrackStateLineAppearanceConfig,
                            *, is_confirmed: bool, is_matched: bool) -> DashedLineConfig:
    if not is_confirmed and is_matched:
        return parent_config.unconfirmed_matched
    elif not is_confirmed and not is_matched:
        return parent_config.unconfirmed_unmatched
    elif is_confirmed and not is_matched:
        return parent_config.confirmed_unmatched
    else:
        raise ValueError(
            "Can't return dash config: both is_confirmed and is_matched are False (corresponds to a solid line)"
        )


def _draw_solid_bbox(img: NDArray[uint8], bbox: BoundingBox, *, color: ColorTuple, thickness: int):
    pt1: PixelPosition = to_px((bbox.x1, bbox.y1))
    pt2: PixelPosition = to_px((bbox.x2, bbox.y2))
    cv2.rectangle(img=img, pt1=pt1, pt2=pt2,
                  color=color, thickness=thickness, lineType=LINE_TYPE)


def _draw_dashed_bbox(img: NDArray[uint8], bbox: BoundingBox,
                      *, color: ColorTuple, thickness: int,
                      dash_config: DashedLineConfig):
    pt1: PixelPosition = to_px((bbox.x1, bbox.y1))
    pt2: PixelPosition = to_px((bbox.x2, bbox.y2))
    dashed_rectangle(img, pt1, pt2,
                     dash=dash_config.dash_length,
                     gap=dash_config.gap_length,
                     color=color, thickness=thickness,
                     lineType=LINE_TYPE)


def _draw_track_state_bbox(img: NDArray[uint8],
                           px_bbox: BoundingBox,
                           *,
                           color: ColorTuple,
                           is_confirmed: bool, is_matched: bool,
                           border_config: TrackStateLineAppearanceConfig,
                           ):
    # if not is_confirmed and not is_matched:
    #     raise ValueError("Invalid track state: at least one of is_confirmed, is_matched must be True")

    bbox_thickness: int = border_config.thickness

    if is_confirmed and is_matched:
        # track confirmed and object detected
        _draw_solid_bbox(img, px_bbox, color=color, thickness=bbox_thickness)
    else:
        dash_config: DashedLineConfig = _get_dashed_line_config(
            border_config, is_confirmed=is_confirmed, is_matched=is_matched
        )
        _draw_dashed_bbox(img, px_bbox, color=color, thickness=bbox_thickness,
                          dash_config=dash_config)


def _draw_vehicle_type_on_bbox(img: NDArray[uint8], vehicle_type: VehicleType, px_bbox: BoundingBox,
                               *, config: ClassIDConfig,
                               classid_bg_color: ColorTuple,
                               classid_text_color: ColorTuple):
    text_len: int = config.display_length
    # truncate; pad with spaces if shorter; align left
    class_id_text: str = vehicle_type.upper()
    text: str = f"{class_id_text[:text_len]: <{text_len}}"
    textbox_config: ColorlessTextboxConfig = config.textbox
    # position: outside bbox, anchored to: top left corner, by: bottom left corner
    anchor: PixelPosition = to_px((px_bbox.x1, px_bbox.y1))
    anchor_line_with_bg(img, text,
                        anchor=anchor,
                        which="bl",
                        offset=textbox_config.offset,
                        padding=textbox_config.padding,
                        bg_color=classid_bg_color,
                        font_color=classid_text_color,
                        font_face=textbox_config.font_face,
                        font_scale=textbox_config.font_scale,
                        thickness=textbox_config.thickness,
                        line_type=LINE_TYPE)


def _draw_track_id_on_bbox(img: NDArray[uint8], track_id: str, px_bbox: BoundingBox,
                           *, config: TrackIDConfig,
                           trackid_bg_color: ColorTuple,
                           trackid_text_color: ColorTuple):
    text_len: int = config.display_length
    # truncate; pad with spaces if shorter; align right
    text: str = f"{track_id[:text_len]: >{text_len}}"
    textbox_config: ColorlessTextboxConfig = config.textbox
    # position: outside bbox, top right corner, anchored by own bottom-right corner
    anchor: PixelPosition = to_px((px_bbox.x2, px_bbox.y1))
    anchor_line_with_bg(img, text,
                        anchor=anchor,
                        which="br",
                        offset=textbox_config.offset,
                        padding=textbox_config.padding,
                        bg_color=trackid_bg_color,
                        font_color=trackid_text_color,
                        font_face=textbox_config.font_face,
                        font_scale=textbox_config.font_scale,
                        thickness=textbox_config.thickness,
                        line_type=LINE_TYPE)
