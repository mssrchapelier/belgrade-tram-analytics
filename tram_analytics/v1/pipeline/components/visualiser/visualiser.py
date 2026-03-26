from typing import Tuple, List, NamedTuple, Dict, Set, TypeAlias, Iterator
from itertools import pairwise
from datetime import datetime

import numpy as np
from numpy import uint8, float64, int64
from numpy.typing import NDArray
import cv2

from common.utils.custom_types import ColorTuple, PlanarPosition, PixelPosition
from common.utils.img.cv2.pretty_put_text import anchor_line_with_bg
from common.utils.img.cv2.drawing import draw_cross, to_px, dashed_line
from common.utils.random.choose_unique_forever import choose_unique_forever
from tram_analytics.v1.models.common_types import VehicleType, BoundingBox
from tram_analytics.v1.models.components.frame_ingestion import Frame
from tram_analytics.v1.models.components.tracking import TrackState, TrackHistory
from tram_analytics.v1.models.components.vehicle_info import (
    VehicleInfo, TramRefPoints, CarRefPoints, PositionContainer
)
from tram_analytics.v1.pipeline.components.vehicle_info.components.zones.zones_config import ZonesConfig, RailTrackCoordsConfig
from tram_analytics.v1.pipeline.components.visualiser.config.colour_palette import (
    TrackLineMarkerColorPalette, TrackColourPalette, TrackColourPaletteItem
)
from tram_analytics.v1.pipeline.components.visualiser.config.visualiser_config import (
    SingleROIVisualisationConfig, FrameOverlayConfig,
    FrameOverlayTextboxConfig, TrackConfig, TrackStateConfig, VisualiserConfig
)
from tram_analytics.v1.pipeline.components.visualiser.settings import (
    PROXY_POINT_COLOR, PROXY_POINT_SIZE, PROXY_POINT_THICKNESS, LINE_TYPE, BboxColours, CLASS_COLOURS
)
from tram_analytics.v1.pipeline.components.visualiser.visualiser_utils import (
    bgr_render_as_grey, RailTrackNumpy, draw_rail_track, _draw_vehicle_speed, _get_track_line_color,
    _get_track_marker_color, _draw_track_segment, _draw_marker, _draw_track_state_bbox, _draw_vehicle_type_on_bbox,
    _draw_track_id_on_bbox
)

# detector id -> coords
ROIMap: TypeAlias = Dict[str, List[PlanarPosition]]

class VehicleStateDrawingData(NamedTuple):
    """
    All information needed to draw the current state of a vehicle
    (bbox, reference points, speed estimates, zone assignments).
    """
    vehicle_id: str
    # tracker bbox, whether is matched with an actual detection
    track_state: TrackState
    # speed, zone assignments, reference points
    vehicle_info: VehicleInfo

def _map_track_states_to_vehicle_info(
        track_states: List[TrackState], vehicle_infos: List[VehicleInfo]
) -> List[VehicleStateDrawingData]:
    """
    Maps track state objects to vehicle info objects,
    combining the information for both in instances of `VehicleStateDrawingData`
    (per vehicle).
    Needed for conveniently drawing this combined information.
    """
    # map by vehicle id

    vehicle_id_to_track_state: Dict[str, TrackState] = {
        item.track_id: item
        for item in track_states
    }
    vehicle_id_to_vehicle_info: Dict[str, VehicleInfo] = {
        item.vehicle_id: item
        for item in vehicle_infos
    }
    # Find vehicle IDs in both sets.
    # By design, the set of IDs in both should be the same,
    # but still constructing robustly.
    common_vehicle_ids: Set[str] = set.intersection(
        set(vehicle_id_to_track_state.keys()),
        set(vehicle_id_to_vehicle_info.keys())
    )
    # build the mappings
    out_objs: List[VehicleStateDrawingData] = [
        VehicleStateDrawingData(
            vehicle_id=vehicle_id,
            track_state=vehicle_id_to_track_state[vehicle_id],
            vehicle_info=vehicle_id_to_vehicle_info[vehicle_id]
        )
        for vehicle_id in common_vehicle_ids
    ]
    return out_objs


class Visualiser:

    def __init__(
            self, config: VisualiserConfig,
            track_color_config: TrackColourPalette,
            *, src_img_size: Tuple[int, int],
            roi_map: ROIMap | None = None,
            zones_config: ZonesConfig | None = None
    ) -> None:

        self._config: VisualiserConfig = config

        self._src_size: Tuple[int, int] = src_img_size
        self._dest_size, self._scale = self._get_dest_size_and_scale(
            src_img_size, self._config.out_height
        )  # type: Tuple[int, int], float

        self._track_color_palette: TrackColourPalette = track_color_config

        # Samples COLOR_PALETTE without replacement until all colours have been exhausted.
        # After that, samples anew.
        # This ensures that there are as few repetitions of colours as possible.
        self._track_colour_iter: Iterator[TrackColourPaletteItem] = choose_unique_forever(
            self._track_color_palette.root
        )

        # detector_id -> DetectorROIVisualizationConfig
        self._roi_configs: Dict[str, SingleROIVisualisationConfig] | None = (
            {config.detector_id: config for config in self._config.roi.root}
            if self._config.roi is not None
            else None
        )
        if self._roi_configs is None and roi_map is not None:
            raise ValueError("Cannot pass roi_map with empty roi in VisualiserConfig")
        # detector ID -> ROI vertices in RESIZED image coordinates
        self._roi_coords_resized: ROIMap | None = self._get_resized_roi_map(roi_map)

        self._rail_tracks: List[RailTrackNumpy] = [
            self._build_rail_track_numpy(track_config.coords)
            for track_config in zones_config.tracks.zones
        ] if zones_config is not None else []

        # track_id -> TrackColorPaletteItem
        self._track_colour_map: Dict[str, TrackColourPaletteItem] = dict()

    @staticmethod
    def _get_dest_size_and_scale(
            src_size: Tuple[int, int], out_height: int | None
    ) -> Tuple[Tuple[int, int], float]:
        """
        Calculate the size for the canvas and the scale to apply to all source coordinates.
        """
        scale: float = out_height / src_size[1] if out_height is not None else 1.0
        dest_w: int = round(src_size[0] * scale)
        dest_h: int = round(src_size[1] * scale)
        dest_size: Tuple[int, int] = (dest_w, dest_h)
        return dest_size, scale

    def _resize_roi(self, roi: List[PlanarPosition]) -> List[PlanarPosition]:
        # resize to new canvas size and round to integers
        new_coords: List[PlanarPosition] = [
            (x_old * self._scale, y_old * self._scale)
            for x_old, y_old in roi
        ]
        return new_coords

    def _get_resized_roi_map(self, roi_map_src: ROIMap | None) -> ROIMap | None:
        if roi_map_src is None:
            return None
        return {
            detector_id: self._resize_roi(old_roi)
            for detector_id, old_roi in roi_map_src.items()
        }

    def _build_rail_track_numpy(self, config: RailTrackCoordsConfig) -> RailTrackNumpy:
        if self._scale is None:
            raise RuntimeError("Called _build_rail_track_numpy with _scale set to None")
        polygon: NDArray[float64] = (
            np.array(config.polygon, dtype=float64) * self._scale
        )
        polygon_int: NDArray[int64] = polygon.astype(int64)
        centreline: NDArray[float64] = (
            np.array(config.centreline, dtype=float64) * self._scale
        )
        centreline_int: NDArray[int64] = centreline.astype(int64)
        corridor: RailTrackNumpy = RailTrackNumpy(
            polygon=polygon_int, centreline=centreline_int
        )
        return corridor

    def _get_colours_for_track(self, track_id: str) -> TrackColourPaletteItem:
        if track_id in self._track_colour_map:
            return self._track_colour_map[track_id]
        # if not in the colour map, generate a new mapping
        new_colour: TrackColourPaletteItem = next(self._track_colour_iter)
        self._track_colour_map[track_id] = new_colour
        return new_colour

    def _resize_canvas(self, canvas: NDArray[uint8]) -> NDArray[uint8]:
        resized: NDArray[uint8] = cv2.resize(
            src=canvas, dsize=None, fx=self._scale, fy=self._scale
        ).astype(uint8, copy=False)
        return resized

    def _transform_canvas(self, canvas: NDArray[uint8]) -> NDArray[uint8]:
        # to greyscale, upsample
        transformed: NDArray[uint8] = canvas
        if self._config.to_greyscale:
            # to greyscale
            transformed = bgr_render_as_grey(canvas)
        # upsample / downsample
        transformed = self._resize_canvas(transformed)
        return transformed

    # --- scaling to canvas ---

    def _scale_planar_pos(self, src: PlanarPosition) -> PlanarPosition:
        return (
            src[0] * self._scale,
            src[1] * self._scale
        )

    def _scale_bbox(self, bbox: BoundingBox) -> BoundingBox:
        return BoundingBox(
            x1=bbox.x1 * self._scale,
            y1=bbox.y1 * self._scale,
            x2=bbox.x2 * self._scale,
            y2=bbox.y2 * self._scale
        )

    # --- conversion between DTOs ---
    # Currently used because the base `VisualizerV2` class uses the old DTO format.
    # To be removed once all implementation is moved from `VisualizerV2` here.

    # --- drawing ---

    @staticmethod
    def _draw_single_roi(img: NDArray[uint8], roi: List[PlanarPosition],
                         config: SingleROIVisualisationConfig) -> None:
        if len(roi) == 0:
            return
        vertex_pairs: List[Tuple[PlanarPosition, PlanarPosition]] = list(pairwise(roi))
        # also connect the last vertex with the first one
        vertex_pairs.append((roi[-1], roi[0]))
        for start, end in vertex_pairs:  # type: PlanarPosition, PlanarPosition
            dashed_line(img, to_px(start), to_px(end),
                        dash=config.dash_length, gap=config.gap_length,
                        color=config.color, thickness=config.thickness,
                        lineType=LINE_TYPE)

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
            roi: List[PlanarPosition] = self._roi_coords_resized[detector_id]
            self._draw_single_roi(img, roi, config)

    def _draw_rail_tracks(self, img: NDArray[uint8]) -> None:
        for rail_track in self._rail_tracks: # type: RailTrackNumpy
            draw_rail_track(img, rail_track)

    def _draw_track(self, img: NDArray[uint8], track_history: TrackHistory) -> None:
        """
        Draw a single vehicle's trajectory (a segmented line joining centres of historical bboxes).

        The positions and the joining segments are coloured differently
        depending on the specific track state's status at that moment (confirmed; matched).

        Sets of colours are chosen randomly from the configured palette
        so that there are as few collisions as possible.
        """
        # the colour configuration for this track, for lines and markers
        color_config: TrackLineMarkerColorPalette = (
            self._get_colours_for_track(track_history.track_id).lines_markers
        )
        states: List[TrackState] = track_history.history
        config: TrackConfig = self._config.track

        # TODO: refactor this (split into different functions), but ensure that
        #   markers are drawn on top of the line segments
        #   (i. e. first draw all segments, then all markers)

        line_thickness: int = config.line_thickness

        # scale to canvas
        bboxes_scaled: List[BoundingBox] = [self._scale_bbox(state.bbox) for state in states]
        # centroids of all states, as pixel coordinates (i. e. rounded to the nearest integer)
        centroids: List[PixelPosition] = [to_px(bbox.centroid)
                                          for bbox in bboxes_scaled]
        marker_colors: List[ColorTuple] = [
            _get_track_marker_color(color_config,
                                    is_confirmed=state.is_confirmed_track,
                                    is_matched=state.is_matched)
            for state in states
        ]
        # [ (centroid_1, centroid_2), (centroid_2, centroid_3), ... ]
        segment_termini_tuples: List[Tuple[PixelPosition, PixelPosition]] = [
            (start, end)
            for start, end in pairwise(centroids)
        ]
        # NOTE: extraction of is_confirmed, is_matched is based on both the START and END points of each segment.
        # The combined attribute is True only if it is True for both the start and the end points of the segment.
        segments_confirmed_flags: List[bool] = [
            start_state.is_confirmed_track and end_state.is_confirmed_track
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
        for start_end_tuple, line_color in zip(segment_termini_tuples,
                                               line_colors):  # type: Tuple[PixelPosition, PixelPosition], ColorTuple
            start, end = start_end_tuple  # type: PixelPosition, PixelPosition
            _draw_track_segment(img, start, end, color=line_color, thickness=line_thickness)
        # ---  draw markers
        for centroid, marker_color in zip(centroids, marker_colors):  # type: PixelPosition, ColorTuple
            _draw_marker(img=img, center=centroid,
                         marker_size=config.marker_size, color=marker_color)

    def _draw_cross_at_point(self, img: NDArray[uint8], pt_unscaled: PlanarPosition) -> None:
        """
        Draw a cross at a specific pixel position. Used to mark reference points for vehicles.
        """

        # TODO: add drawing params for reference points to configuration
        # NOTE: not static because planning to pull params from config stored in an instance field

        # scale coords to match the canvas size
        pt_scaled: PlanarPosition = self._scale_planar_pos(pt_unscaled)
        # converting coords to integers
        pixel_pt_scaled: Tuple[int, int] = (round(pt_scaled[0]), round(pt_scaled[1]))
        # drawing the cross
        draw_cross(img, center=pixel_pt_scaled, size=PROXY_POINT_SIZE,
                   color=PROXY_POINT_COLOR, thickness=PROXY_POINT_THICKNESS,
                   lineType=LINE_TYPE)

    def _draw_car_refpoints(self, img: NDArray[uint8], refpoints: CarRefPoints) -> None:
        """
        Draw reference points for a car (currently configured: the lower border midpoint of the tracker bbox).
        """
        # point to draw: lower border midpoint, image coordinates
        # (also has bbox centroid, but was already drawn when drawing the track history)
        pt: PlanarPosition = refpoints.bbox_lower_border_midpoint.image
        self._draw_cross_at_point(img, pt)

    def _draw_tram_refpoints(self, img: NDArray[uint8], refpoints: TramRefPoints) -> None:
        """
        Draw reference points for a car (as currently configured: 1. the projections of the tracker bbox's
        intersection points with the assigned track polygon onto the track's centreline -- the forwardmost
        and the backwarmost point; 2. the midpoint between the two as defined in *world* coordinates,
        if available).
        """
        # points to draw:
        # bbox projections onto the track centerline (if present)
        # -- start, end, and centre in image plane
        if refpoints.vehicle_centreline is not None:
            containers_to_draw: List[PositionContainer | None] = [
                refpoints.vehicle_centreline.start,
                # TODO: configure to draw the centre in image plane
                #  if the centre in world plane is not available (if a homography was not defined)
                refpoints.vehicle_centreline.centre_in_world_plane,
                refpoints.vehicle_centreline.end
            ]
            for container in containers_to_draw: # type: PositionContainer | None
                if container is not None:
                    self._draw_cross_at_point(img, container.image)

    def _draw_reference_points(self, img: NDArray[uint8], vehicle_info: VehicleInfo) -> None:
        match vehicle_info.vehicle_type:
            case VehicleType.CAR:
                self._draw_car_refpoints(img, vehicle_info.reference_points)
            case VehicleType.TRAM:
                self._draw_tram_refpoints(img, vehicle_info.reference_points)
            case _:
                raise ValueError(f"Unknown vehicle type: {vehicle_info.vehicle_type}")

    def _draw_track_state(self, img: NDArray[uint8], state: TrackState,
                          *, track_id: str, vehicle_type: VehicleType,
                          bbox_colors: BboxColours,
                          trackid_bg_color: ColorTuple,
                          trackid_text_color: ColorTuple,
                          config: TrackStateConfig):
        border_color: ColorTuple = bbox_colors.border_color
        bbox_scaled: BoundingBox = self._scale_bbox(state.bbox)
        # draw the bounding box
        _draw_track_state_bbox(img, bbox_scaled,
                               is_confirmed=state.is_confirmed_track,
                               is_matched=state.is_matched,
                               border_config=config.bbox_border, color=border_color)
        # draw the class ID
        _draw_vehicle_type_on_bbox(img, vehicle_type, bbox_scaled,
                                   config=config.bbox_text.class_id,
                                   classid_bg_color=bbox_colors.classid_bg_color,
                                   classid_text_color=bbox_colors.classid_text_color)
        # draw the track ID
        _draw_track_id_on_bbox(img, track_id, bbox_scaled,
                               config=config.bbox_text.track_id,
                               trackid_bg_color=trackid_bg_color,
                               trackid_text_color=trackid_text_color)

    def _draw_vehicle_state(
            self, img: NDArray[uint8], state_data: VehicleStateDrawingData
    ) -> None:
        # colours for the track id annotation
        colours_for_track: TrackColourPaletteItem = self._get_colours_for_track(
            state_data.vehicle_id
        )
        # colours for the bbox border and class id annotation
        bbox_colours: BboxColours = CLASS_COLOURS[
            state_data.track_state.vehicle_type
        ]

        # draw the bounding box, class ID, track ID
        self._draw_track_state(img, state_data.track_state,
                               track_id=state_data.vehicle_id,
                               vehicle_type=state_data.track_state.vehicle_type,
                               trackid_bg_color=colours_for_track.trackid_bg_color,
                               trackid_text_color=colours_for_track.trackid_text_color,
                               bbox_colors=bbox_colours,
                               config=self._config.track_state)
        # draw reference points
        self._draw_reference_points(img, state_data.vehicle_info)
        # draw speeds
        # - value to display: smoothed
        _draw_vehicle_speed(img,
                            state_data.vehicle_info.speeds.smoothed,
                            px_bbox=self._scale_bbox(state_data.track_state.bbox),
                            config=self._config.speed,
                            bg_color=colours_for_track.trackid_bg_color,
                            text_color=colours_for_track.trackid_text_color)

    def _draw_frame_info(self, img: NDArray[uint8], *, frame_id: str, timestamp: datetime):
        # overlay: black background in the top left corner of the frame
        # content: "DD.MM.YYYY HH:MM:SS.SSS (GMT) | frame_id[:6]"
        config: FrameOverlayConfig = self._config.frame_overlay
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

    def process_frame(self,
                      *, frame: Frame,
                      track_histories: List[TrackHistory],
                      vehicle_infos: List[VehicleInfo]) -> NDArray[uint8]:
        canvas: NDArray[uint8] = frame.image.copy()
        canvas = self._transform_canvas(canvas)

        # draw the regions of interest for the detectors
        self._draw_all_roi(canvas)
        # draw rail corridors
        self._draw_rail_tracks(canvas)
        # TODO: implement drawing platforms, intrusion zones from vehicle_infos

        # last track state in each track history
        last_track_states: List[TrackState] = [
            track_history.history[-1]
            for track_history in track_histories
        ]
        # map, by vehicle ID, the last track state in each history
        # to vehicle info objects containing derived calculated info
        last_states_data: List[VehicleStateDrawingData] = (
            _map_track_states_to_vehicle_info(last_track_states, vehicle_infos)
        )

        # draw all tracks -- each as a line formed by the vehicle's bbox centre movement,
        # and markers corresponding to the bbox centre's historical positions
        for track_history in track_histories: # type: TrackHistory
            # draw the track as a line and markers
            self._draw_track(canvas, track_history)

        # draw all current positions for vehicles (bboxes, reference points, speeds)
        for state_data in last_states_data: # type: VehicleStateDrawingData
            self._draw_vehicle_state(canvas, state_data)

        # draw frame info (frame id, timestamp)
        self._draw_frame_info(canvas, frame_id=frame.frame_id,
                              timestamp=frame.timestamp)

        return canvas
