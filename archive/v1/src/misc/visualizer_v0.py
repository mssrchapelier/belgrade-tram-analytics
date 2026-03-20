from typing import List, Dict, Tuple, TypeAlias, Iterator
from datetime import datetime
from itertools import pairwise
from warnings import deprecated

import numpy as np
from numpy import uint8
from numpy.typing import NDArray
from pydantic import BaseModel
import cv2
from PIL import Image, ImageFile

from common.utils.img.cv2.pretty_put_text import anchor_line_with_bg
from tram_analytics.v1.pipeline.components.visualiser.config.colour_palette import (
    TrackColourPalette, TrackColourPaletteItem, TrackLineMarkerColorPalette,
    LineColorPaletteItem
)
from tram_analytics.v1.models.common_types import BoundingBox, VehicleType
from tram_analytics.v1.models.components.frame_ingestion import Frame
from common.utils.custom_types import ColorTuple
from common.utils.img.img_bytes_conversion import bytes_from_pil
from archive.common.utils.img.img_bytes_conversion import pil_from_bytes_old
from common.utils.random.choose_unique_forever import choose_unique_forever
from common.utils.img.cv2.drawing import dashed_rectangle, dashed_line
from tram_analytics.v1.pipeline.components.visualiser.config.visualiser_config import (
    VisualiserConfig, FrameOverlayConfig, FrameOverlayTextboxConfig, TrackConfig,
    DashedLineConfig, TrackStateConfig, TrackStateLineAppearanceConfig,
    ColorlessTextboxConfig, ClassIDConfig, TrackIDConfig,
    SingleROIVisualisationConfig
)
from archive.v1.src.v_0_2_0.pipeline.components.visualizer_input_builder import TrackState, TrackWithHistory


class BboxColours(BaseModel):
    border_color: ColorTuple

    # text for: class_id
    classid_bg_color: ColorTuple
    classid_text_color: ColorTuple

CLASS_COLOURS: Dict[VehicleType, BboxColours] = {
    VehicleType.TRAM: BboxColours(
        border_color=(64, 0, 191), # BF0040 (dark pink)
        classid_bg_color=(255, 255, 255), # white
        classid_text_color=(64, 0, 191) # BF0040 (dark pink)
    ),
    VehicleType.CAR: BboxColours(
        border_color=(255, 152, 17), # 0FA1FF (blue nebula)
        classid_bg_color=(255, 255, 255), # white
        classid_text_color=(255, 152, 17) # 0FA1FF (blue nebula)
    )
}

PixelPoint: TypeAlias = Tuple[int, int]
FloatPoint: TypeAlias = Tuple[float, float]

def to_px(float_point: FloatPoint) -> PixelPoint:
    return round(float_point[0]), round(float_point[1])

# detector ID -> ROI vertices
ROIMap_Float: TypeAlias = Dict[str, List[FloatPoint]]

LINE_TYPE: int = cv2.LINE_8

OUT_PIL_IMG_FORMAT: str = "PNG"


class ROI(BaseModel):
    detector_id: str
    color: ColorTuple


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

def _draw_track_segment(img: NDArray[np.uint8], start: PixelPoint, end: PixelPoint,
                        *, color: ColorTuple, thickness: int):
    cv2.line(img=img, pt1=start, pt2=end, color=color,
                 thickness=thickness, lineType=LINE_TYPE)

def _draw_marker(img: NDArray[np.uint8], center: PixelPoint,
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

def _draw_track(img: NDArray[np.uint8], states: List[TrackState], *,
                color_config: TrackLineMarkerColorPalette,
                config: TrackConfig):
    # TODO: refactor this (split into different functions), but ensure that
    #   markers are drawn on top of the line segments
    #   (i. e. first draw all segments, then all markers)

    line_thickness: int = config.line_thickness

    # centroids of all states, as pixel coordinates (i. e. rounded to the nearest integer)
    centroids: List[PixelPoint] = [
        to_px(state.bbox.centroid)
        for state in states
    ]
    marker_colors: List[ColorTuple] = [
        _get_track_marker_color(color_config,
                                is_confirmed=state.is_confirmed,
                                is_matched=state.is_matched)
        for state in states
    ]
    # [ (centroid_1, centroid_2), (centroid_2, centroid_3), ... ]
    segment_termini_tuples: List[Tuple[PixelPoint, PixelPoint]] = [
        (start, end)
        for start, end in pairwise(centroids)
    ]
    # NOTE: extraction of is_confirmed, is_matched is based on both the START and END points of each segment.
    # The combined attribute is True only if it is True for both the start and the end points of the segment.
    segments_confirmed_flags: List[bool] = [
        start_state.is_confirmed and end_state.is_confirmed
        for start_state, end_state in pairwise(states)
    ]
    segments_matched_flags: List[bool] = [
        start_state.is_matched and end_state.is_matched
        for start_state, end_state in pairwise(states)
    ]
    line_colors: List[ColorTuple] = [
        _get_track_line_color(color_config,
                              is_confirmed=is_confirmed, is_matched=is_matched)
        for is_confirmed, is_matched in zip(segments_confirmed_flags, segments_matched_flags)
    ]

    # draw track lines, then markers on top of them
    # ---  draw track lines
    for start_end_tuple, line_color in zip(segment_termini_tuples, line_colors): # type: Tuple[PixelPoint, PixelPoint], ColorTuple
        start, end = start_end_tuple # type: PixelPoint, PixelPoint
        _draw_track_segment(img, start, end, color=line_color, thickness=line_thickness)
    # ---  draw markers
    for centroid, marker_color in zip(centroids, marker_colors):  # type: PixelPoint, ColorTuple
        _draw_marker(img=img, center=centroid,
                     marker_size=config.marker_size, color=marker_color)

@deprecated("Deprecated, use Visualizer._resize_bbox() instead")
def _resize_bbox_old(bbox: BoundingBox,
                     old_img_size: Tuple[int, int],
                     new_img_size: Tuple[int, int]) -> BoundingBox:

    if old_img_size == new_img_size:
        return bbox.model_copy()

    w_old, h_old = old_img_size # type: int, int
    w_new, h_new = new_img_size # type: int, int

    # convert to relative coordinates
    x1_abs_old, y1_abs_old, x2_abs_old, y2_abs_old = bbox.to_xyxy_list() # type: float, float, float, float
    x1_rel, x2_rel = x1_abs_old / w_old, x2_abs_old / w_old # type: float, float
    y1_rel, y2_rel = y1_abs_old / h_old, y2_abs_old / h_old # type: float, float
    x1_abs_new: int = round(x1_rel * w_new)
    x2_abs_new: int = round(x2_rel * w_new)
    y1_abs_new: int = round(y1_rel * h_new)
    y2_abs_new: int = round(y2_rel * h_new)

    new_bbox: BoundingBox = BoundingBox(x1=x1_abs_new, x2=x2_abs_new,
                                        y1=y1_abs_new, y2=y2_abs_new)
    return new_bbox

@deprecated("Deprecated, use Visualizer._transform_roi() instead")
def _resize_roi_old(roi: List[FloatPoint],
                    old_img_size: Tuple[int, int],
                    new_img_size: Tuple[int, int]) -> List[PixelPoint]:
    w_old, h_old = old_img_size  # type: int, int
    w_new, h_new = new_img_size  # type: int, int
    new_coords: List[PixelPoint] = [
        (round(x_old * w_new / w_old), round(y_old * w_new / w_old))
        for x_old, y_old in roi
    ]
    return new_coords

@deprecated("Deprecated, use Visualizer._resize_coords() instead")
def _resize_coords_in_track_old(track: TrackWithHistory,
                                old_img_size: Tuple[int, int],
                                new_img_size: Tuple[int, int]):
    resized_states: List[TrackState] = [
        TrackState(is_matched=state.is_matched,
                   is_confirmed=state.is_confirmed,
                   bbox=_resize_bbox_old(state.bbox, old_img_size, new_img_size))
        for state in track.history
    ]
    new_track: TrackWithHistory = TrackWithHistory(track_id=track.track_id,
                                                   class_id=track.class_id,
                                                   history=resized_states)
    return new_track

def _img_bytes_to_numpy(img: bytes) -> NDArray[uint8]:
    # currently through PIL, find a better way perhaps
    with pil_from_bytes_old(img) as pil_img:  # type: ImageFile
        img_numpy: NDArray[np.uint8] = np.array(pil_img)
        # RGB -> BGR
        img_numpy = cv2.cvtColor(img_numpy, cv2.COLOR_RGB2BGR)
    return img_numpy

def _numpy_to_img_bytes(img_bgr: NDArray[uint8]) -> bytes:
    img_rgb: NDArray[uint8] = cv2.cvtColor(img_bgr, cv2.COLOR_BGR2RGB)
    with Image.fromarray(img_rgb) as pil_img: # type: ImageFile
        img_bytes: bytes = bytes_from_pil(pil_img, img_format=OUT_PIL_IMG_FORMAT)
    return img_bytes

def _bgr_render_as_gray(img_bgr: NDArray[uint8]) -> NDArray[uint8]:
    # to 1-dim greyscale
    gray: NDArray[uint8] = cv2.cvtColor(src=img_bgr, code=cv2.COLOR_BGR2GRAY)
    # back to 3-dim BGR (but now a greyscale image)
    gray = cv2.cvtColor(src=gray, code=cv2.COLOR_GRAY2BGR)
    return gray

@deprecated("Deprecated, use Visualizer._resize_canvas() instead")
def _resize_to_height_old(img: NDArray[uint8], new_height: int) -> NDArray[uint8]:
    old_height: int = img.shape[0]
    if old_height == new_height:
        return img
    scaling_factor: float = new_height / old_height
    resized: NDArray[uint8] = cv2.resize(src=img, dsize=None, fx=scaling_factor, fy=scaling_factor)
    return resized

def _draw_frame_info(img: NDArray[uint8], *, frame_id: str, timestamp: datetime,
                     config: FrameOverlayConfig):
    # overlay: black background in the top left corner of the frame
    # content: "DD.MM.YYYY HH:MM:SS.SSS (GMT) | frame_id[:6]"

    f_id_len: int = config.frame_id_display_length
    # truncate frame_id; pad with spaces if shorter
    frame_id_text: str = f"{frame_id[:f_id_len]: <{f_id_len}}"
    ts_text: str = timestamp.strftime(config.timestamp_format)
    text: str = f"{frame_id_text} | {ts_text}"
    text_config: FrameOverlayTextboxConfig = config.textbox
    anchor_line_with_bg(img, text,
                        anchor=text_config.anchor,
                        which=text_config.which_corner,
                        offset=text_config.offset,
                        padding=text_config.padding,
                        bg_color=text_config.bg_color,
                        font_color=text_config.font_color,
                        font_face=text_config.font_face,
                        font_scale=text_config.font_scale,
                        thickness=text_config.thickness,
                        line_type=LINE_TYPE)

def _draw_solid_bbox(img: NDArray[uint8], bbox: BoundingBox, *, color: ColorTuple, thickness: int):
    pt1: PixelPoint = to_px((bbox.x1, bbox.y1))
    pt2: PixelPoint = to_px((bbox.x2, bbox.y2))
    cv2.rectangle(img=img, pt1=pt1, pt2=pt2,
                  color=color, thickness=thickness, lineType=LINE_TYPE)

def _draw_dashed_bbox(img: NDArray[uint8], bbox: BoundingBox,
                      *, color: ColorTuple, thickness: int,
                      dash_config: DashedLineConfig):
    pt1: PixelPoint = to_px((bbox.x1, bbox.y1))
    pt2: PixelPoint = to_px((bbox.x2, bbox.y2))
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
    anchor: PixelPoint = to_px((px_bbox.x1, px_bbox.y1))
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
    anchor: PixelPoint = to_px((px_bbox.x2, px_bbox.y1))
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

def _draw_track_state(img: NDArray[uint8], state: TrackState,
                      *, track_id: str, vehicle_type: VehicleType,
                      bbox_colors: BboxColours,
                      trackid_bg_color: ColorTuple,
                      trackid_text_color: ColorTuple,
                      config: TrackStateConfig):
    border_color: ColorTuple = bbox_colors.border_color
    px_bbox: BoundingBox = state.bbox
    # draw the bounding box
    _draw_track_state_bbox(img, px_bbox,
                           is_confirmed=state.is_confirmed,
                           is_matched=state.is_matched,
                           border_config=config.bbox_border, color=border_color)
    # draw the class ID
    _draw_vehicle_type_on_bbox(img, vehicle_type, px_bbox,
                               config=config.bbox_text.class_id,
                               classid_bg_color=bbox_colors.classid_bg_color,
                               classid_text_color=bbox_colors.classid_text_color)
    # draw the track ID
    _draw_track_id_on_bbox(img, track_id, px_bbox,
                           config=config.bbox_text.track_id,
                           trackid_bg_color=trackid_bg_color,
                           trackid_text_color=trackid_text_color)

def _draw_single_roi(img: NDArray[uint8], roi: List[FloatPoint], config: SingleROIVisualisationConfig) -> None:
    if len(roi) == 0:
        return
    vertex_pairs: List[Tuple[FloatPoint, FloatPoint]] = list(pairwise(roi))
    # also connect the last vertex with the first one
    vertex_pairs.append((roi[-1], roi[0]))
    for start, end in vertex_pairs: # type: FloatPoint, FloatPoint
        dashed_line(img, to_px(start), to_px(end),
                    dash=config.dash_length, gap=config.gap_length,
                    color=config.color, thickness=config.thickness,
                    lineType=LINE_TYPE)

class Visualizer:

    def __init__(self,
                 config: VisualiserConfig,
                 track_color_config: TrackColourPalette,
                 *,
                 src_img_size: Tuple[int, int],
                 roi_map: ROIMap_Float | None = None):
        # colours for tracks, to randomly choose from for each new track ID
        # self._track_color_palette: List[TrackColorPaletteItem] = load_palette_from_dir(track_color_config_path)
        self._track_color_palette: TrackColourPalette = track_color_config
        self._track_color_iter: Iterator[TrackColourPaletteItem] = self._get_color_map_iterator(
            self._track_color_palette.root
        )

        # track_id -> TrackColorPaletteItem
        self._track_color_map: Dict[str, TrackColourPaletteItem] = dict()

        self.config: VisualiserConfig = config

        self._src_size: Tuple[int, int] = src_img_size
        self._dest_size, self._scale = self._get_dest_size_and_scale(
            src_img_size, self.config.out_height
        ) # type: Tuple[int, int], float

        # detector_id -> DetectorROIVisualizationConfig
        self._roi_configs: Dict[str, SingleROIVisualisationConfig] = (
            {config.detector_id: config for config in self.config.roi.root}
            if self.config.roi is not None
            else None
        )
        if self._roi_configs is None and roi_map is not None:
            raise ValueError("Cannot pass roi_map with empty roi in VisualizerConfig")
        # detector ID -> ROI vertices in RESIZED image coordinates
        self._roi_coords_resized: ROIMap_Float = self._get_resized_roi_map(roi_map)

    @staticmethod
    def _get_dest_size_and_scale(src_size: Tuple[int, int], out_height: int | None) -> Tuple[Tuple[int, int], float]:
        scale: float = out_height / src_size[1] if out_height is not None else 1.0
        dest_w: int = round(src_size[0] * scale)
        dest_h: int = round(src_size[1] * scale)
        dest_size: Tuple[int, int] = (dest_w, dest_h)
        return dest_size, scale

    @staticmethod
    def _get_color_map_iterator(palette: List[TrackColourPaletteItem]) -> Iterator[TrackColourPaletteItem]:
        # Samples COLOR_PALETTE without replacement until all colours have been exhausted.
        # After that, samples anew.
        # This ensures that there are as few repetitions of colours as possible.
        return choose_unique_forever(palette)

    def _get_colors_for_track(self, track_id: str) -> TrackColourPaletteItem:
        if track_id in self._track_color_map:
            return self._track_color_map[track_id]
        # if not in the colour map, generate a new mapping
        new_color: TrackColourPaletteItem = next(self._track_color_iter)
        self._track_color_map[track_id] = new_color
        return new_color

    def _resize_canvas(self, canvas: NDArray[uint8]) -> NDArray[uint8]:
        canvas = cv2.resize(src=canvas, dsize=None, fx=self._scale, fy=self._scale)
        return canvas

    def _resize_roi(self, roi: List[FloatPoint]) -> List[FloatPoint]:
        # resize to new canvas size and round to integers
        new_coords: List[FloatPoint] = [
            (x_old * self._scale, y_old * self._scale)
            for x_old, y_old in roi
        ]
        return new_coords

    def _get_resized_roi_map(self, roi_map_src: ROIMap_Float | None) -> ROIMap_Float | None:
        if roi_map_src is None:
            return None
        return {
            detector_id: self._resize_roi(old_roi)
            for detector_id, old_roi in roi_map_src.items()
        }

    def _resize_bbox(self, bbox: BoundingBox) -> BoundingBox:
        x1_abs_old, y1_abs_old, x2_abs_old, y2_abs_old = bbox.to_xyxy_list()  # type: float, float, float, float
        x1_abs_new: float = x1_abs_old * self._scale
        x2_abs_new: float = x2_abs_old * self._scale
        y1_abs_new: float = y1_abs_old * self._scale
        y2_abs_new: float = y2_abs_old * self._scale

        new_bbox: BoundingBox = BoundingBox(x1=x1_abs_new, x2=x2_abs_new,
                                            y1=y1_abs_new, y2=y2_abs_new)
        return new_bbox

    def _resize_track(self, track: TrackWithHistory) -> TrackWithHistory:
        resized_states: List[TrackState] = [
            TrackState(is_matched=state.is_matched,
                       is_confirmed=state.is_confirmed,
                       bbox=self._resize_bbox(state.bbox))
            for state in track.history
        ]
        new_track: TrackWithHistory = TrackWithHistory(track_id=track.track_id,
                                                       class_id=track.class_id,
                                                       history=resized_states)
        return new_track

    def _resize_coords_in_tracks(self, tracks: List[TrackWithHistory]) -> List[TrackWithHistory]:
        return [self._resize_track(track) for track in tracks]

    def _transform_canvas(self, canvas: NDArray[uint8]) -> NDArray[uint8]:
        # to greyscale, upsample
        transformed: NDArray[uint8] = canvas
        if self.config.to_greyscale:
            # to greyscale
            transformed = _bgr_render_as_gray(canvas)
        # upsample / downsample
        transformed = self._resize_canvas(transformed)
        return transformed

    def _draw_roi(self, img: NDArray[uint8], *, detector_id: str, roi: List[PixelPoint]):
        # TODO: draw masks once and store in an instance field, then apply to each image --
        #  probably less computationally expensive. Will require the ROI regions being passed at initialisation time.
        pass

    def _draw_frame_info(self, img: NDArray[uint8], *, frame_id: str, timestamp: datetime):
        _draw_frame_info(img, frame_id=frame_id, timestamp=timestamp,
                         config=self.config.frame_overlay)


    def _draw_track_old(self, img: NDArray[uint8], track: TrackWithHistory):
        # the colour configuration for this track, for lines and markers
        color_config: TrackLineMarkerColorPalette = self._get_colors_for_track(track.track_id).lines_markers
        _draw_track(img, track.history,
                    config=self.config.track,
                    color_config=color_config)

    def _draw_track_state(self, img: NDArray[uint8], state: TrackState,
                          *, track_id: str, class_id: int):
        # colours for the track id annotation
        colors_for_track: TrackColourPaletteItem = self._get_colors_for_track(track_id)
        # colours for the bbox border and class id annotation
        bbox_colors: BboxColours = CLASS_COLOURS[class_id]

        _draw_track_state(img, state, track_id=track_id, class_id=class_id,
                          bbox_colors=bbox_colors,
                          trackid_bg_color=colors_for_track.trackid_bg_color,
                          trackid_text_color=colors_for_track.trackid_text_color,
                          config=self.config.track_state)

    @staticmethod
    def _export_canvas(canvas: NDArray[uint8]) -> bytes:
        return _numpy_to_img_bytes(canvas)

    @staticmethod
    def _draw_single_roi(img: NDArray[uint8], roi: List[FloatPoint],
                         config: SingleROIVisualisationConfig) -> None:
        _draw_single_roi(img, roi, config)

    def _draw_all_roi(self, img: NDArray[uint8]) -> None:
        if self._roi_configs is None or self._roi_coords_resized is None:
            return
        detector_ids: List[str] = list(self._roi_coords_resized.keys())
        # ensure the order of drawing: in the ascending alphabetical order of detector IDs
        detector_ids.sort()
        for detector_id in detector_ids: # type: str
            if detector_id not in self._roi_configs:
                raise ValueError(
                    f"Detector ID {detector_id} not present in the visualiser's configuration. "
                    + "IDs present in the configuration: {}".format(
                        ", ".join(sorted(list(self._roi_configs.keys())))
                    ))
            config: SingleROIVisualisationConfig = self._roi_configs[detector_id]
            if detector_id != config.detector_id:
                raise RuntimeError(f"Inconsistent _roi_configs in Visualizer: "
                                   f"detector_id as map key: {detector_id}, as config field: {config.detector_id}")
            roi: List[FloatPoint] = self._roi_coords_resized[detector_id]
            self._draw_single_roi(img, roi, config)

    def process_frame_old(self, frame: Frame, tracks: List[TrackWithHistory]) -> NDArray[np.uint8]:
        # create a copy (do not mutate the original array)
        # NOTE: The original array's `writeable` flag is being set to `False` in `BaseFrameStreamer`,
        # so mutating it should not be possible anyway,
        # but still copying it for extra safety.

        src_img: NDArray[uint8] = frame.image.copy()
        canvas: NDArray[uint8] = src_img.copy()
        canvas: NDArray[uint8] = self._transform_canvas(canvas)

        # draw the regions of interest for the detectors
        self._draw_all_roi(canvas)

        # resize all coordinates in tracks (if _transform_canvas has resized the image)
        # TODO: vectorise (straightforward in code, but very inefficient computationally)
        resized_tracks: List[TrackWithHistory] = self._resize_coords_in_tracks(tracks)
        for track in resized_tracks: # type: TrackWithHistory
            # draw the track as a line and markers
            self._draw_track_old(canvas, track)
            # draw the bounding box and info (class id, track id) for the last state
            self._draw_track_state(canvas, track.history[-1],
                                   track_id=track.track_id,
                                   class_id=track.class_id)

        # draw frame info (frame id, timestamp)
        self._draw_frame_info(canvas, frame_id=frame.frame_id,
                              timestamp=frame.timestamp)

        return canvas
