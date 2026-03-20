__version__ = "0.2.0"

from typing import Dict, List, Tuple, Any
from itertools import pairwise
from operator import itemgetter

import numpy as np
from numpy.typing import NDArray

from archive.v1.src.v_0_2_0.pipeline.components.analytics.scene_geometry.scene_geometry import SceneGeometryConfig
from archive.v1.src.v_0_2_0.pipeline.components.visualizer_input_builder import TrackState, TrackWithHistory
from archive.v1.src.misc.visualizer_v0 import (
    Visualizer, ROIMap_Float, _draw_track_state
)
from archive.v1.src.v_0_2_0.pipeline.components.visualizer_input_builder_v2 import (
    EnhancedTrackState, EnhancedTrackWithHistory, TramEnhancedTrackState
)
from archive.v1.src.v_0_2_0.pipeline.components.analytics.analytics_postprocessor_old import VehicleType
from archive.v1.src.v_0_2_0.pipeline.components.analytics.trams import TramPositionalProxies, RailCorridorConfig
from tram_analytics.v1.pipeline.components.visualiser.config.visualiser_config import VisualiserConfig
from tram_analytics.v1.pipeline.components.visualiser.config.colour_palette import (
    TrackColourPalette, TrackColourPaletteItem
)
from tram_analytics.v1.models.common_types import BoundingBox
from tram_analytics.v1.models.components.frame_ingestion import Frame
from tram_analytics.v1.pipeline.components.visualiser.config.visualiser_config import TrackStateConfig
from common.utils.custom_types import ColorTuple, PlanarPosition, PixelPosition
from common.utils.img.cv2.drawing import draw_cross, dashed_line, to_px
from tram_analytics.v1.pipeline.components.visualiser.settings import PROXY_POINT_COLOR, PROXY_POINT_SIZE, \
    PROXY_POINT_THICKNESS, LINE_TYPE, BboxColours, CLASS_COLOURS, CORRIDOR_DASH_LENGTH, CORRIDOR_GAP_LENGTH, \
    CORRIDOR_COLOUR, CORRIDOR_THICKNESS
from tram_analytics.v1.pipeline.components.visualiser.visualiser_utils import RailTrackNumpy, draw_rail_track


def _draw_rail_corridor_old(img: NDArray, config: RailCorridorConfig):
    # border
    border_pts: List[PlanarPosition] = config.polygon
    border_pt_pairs: List[Tuple[PlanarPosition, PlanarPosition]] = list(pairwise(border_pts))
    # also connect the last vertex with the first one
    border_pt_pairs.append((border_pts[-1], border_pts[0]))
    for start, end in border_pt_pairs: # type: PlanarPosition, PlanarPosition
        dashed_line(img, to_px(start), to_px(end),
                    dash=CORRIDOR_DASH_LENGTH, gap=CORRIDOR_GAP_LENGTH,
                    color=CORRIDOR_COLOUR, thickness=CORRIDOR_THICKNESS,
                    lineType=LINE_TYPE)
    # centre line
    for start, end in pairwise(config.centerline): # type: PlanarPosition, PlanarPosition
        dashed_line(img, to_px(start), to_px(end),
                    dash=CORRIDOR_DASH_LENGTH, gap=CORRIDOR_GAP_LENGTH,
                    color=CORRIDOR_COLOUR, thickness=CORRIDOR_THICKNESS,
                    lineType=LINE_TYPE)

def _track_state_from_enhanced(enhanced: EnhancedTrackState) -> TrackState:
    return TrackState(is_matched=enhanced.is_matched,
                      is_confirmed=enhanced.is_confirmed,
                      bbox=enhanced.bbox)

def _track_from_enhanced(enhanced: EnhancedTrackWithHistory) -> TrackWithHistory:
    return TrackWithHistory(track_id=enhanced.track_id,
                            class_id=enhanced.class_id,
                            history=[_track_state_from_enhanced(state) for state in enhanced.history])

def _draw_enhanced_track_state(img: NDArray, state: EnhancedTrackState,
                               *, track_id: str, class_id: int,
                               bbox_colors: BboxColours,
                               trackid_bg_color: ColorTuple,
                               trackid_text_color: ColorTuple,
                               config: TrackStateConfig):
    nonenhanced_state: TrackState = _track_state_from_enhanced(state)
    # draw the bounding box, class ID, track ID
    _draw_track_state(img, nonenhanced_state, track_id=track_id, class_id=class_id,
                      bbox_colors=bbox_colors,
                      trackid_bg_color=trackid_bg_color, trackid_text_color=trackid_text_color,
                      config=config)
    # for trams, draw proxy points
    if state.vehicle_type == VehicleType.TRAM:
        # TODO: also draw corridor ID perhaps
        proxies: TramPositionalProxies | None = state.proxies
        if proxies is not None:
            for pt in [proxies.proj_span_start,
                       proxies.proj_span_center,
                       proxies.proj_span_end]: # type: PlanarPosition
                pt_px: PixelPosition = to_px(pt)
                draw_cross(img, center=pt_px, size=PROXY_POINT_SIZE,
                           color=PROXY_POINT_COLOR, thickness=PROXY_POINT_THICKNESS,
                           lineType=LINE_TYPE)


class VisualizerV2(Visualizer):

    def __init__(self,
                 config: VisualiserConfig,
                 track_color_config: TrackColourPalette,
                 *, src_img_size: Tuple[int, int],
                 roi_map: ROIMap_Float | None = None,
                 scene_geometry_config: SceneGeometryConfig | None = None
                 ):
        super().__init__(config, track_color_config, src_img_size=src_img_size, roi_map=roi_map)
        # rail corridor ID -> RailCorridor
        self._rail_corridor_configs_by_id: Dict[int, RailCorridorConfig] | None = {
            config.corridor_id: config
            for config in scene_geometry_config.rail_corridors
        } if scene_geometry_config is not None else None
        self._rail_corridors: List[RailTrackNumpy] = self._build_rail_corridors(
            scene_geometry_config.rail_corridors
        )

    def _build_corridor(self, config: RailCorridorConfig):
        if self._scale is None:
            raise RuntimeError("Called _build_corridor with _scale set to None")
        # polygon: ndarray[Tuple[int, Literal[2]], np.float32]
        polygon: NDArray[np.float32] = (
            np.array(config.polygon, dtype=np.float32) * self._scale
        )
        # polygon_int: ndarray[Tuple[int, Literal[2]], np.int32]
        polygon_int: NDArray[np.float32] = polygon.astype(np.int32)
        # centerline: ndarray[Tuple[int, Literal[2]], np.float32]
        centerline: NDArray[np.float32] = (
            np.array(config.centerline, dtype=np.float32) * self._scale
        )
        # centerline_int: ndarray[Tuple[int, Literal[2]], np.int32]
        centerline_int: NDArray[np.int32] = centerline.astype(np.int32)
        corridor: RailTrackNumpy = RailTrackNumpy(
            polygon=polygon_int, centreline=centerline_int
        )
        return corridor

    def _build_rail_corridors(self, configs: List[RailCorridorConfig]) -> List[RailTrackNumpy]:
        return list(map(lambda c: self._build_corridor(c), configs))

    def _draw_rail_corridors_old(self, img: NDArray):
        if self._rail_corridor_configs_by_id is not None:
            # sort by corridor id
            for corridor_id, config in sorted(list(self._rail_corridor_configs_by_id.items()),
                                              key=itemgetter(0)): # type: str, RailCorridorConfig
                _draw_rail_corridor_old(img, config)

    def _draw_rail_corridors(self, img: NDArray):
        if self._rail_corridor_configs_by_id is not None:
            for corridor in self._rail_corridors: # type: RailTrackNumpy
                draw_rail_track(img, corridor)

    def _resize_enhanced_track_state(self, state: EnhancedTrackState) -> EnhancedTrackState:
        is_tram: bool = isinstance(state, TramEnhancedTrackState)
        resized_bbox: BoundingBox = self._resize_bbox(state.bbox)
        fields_to_update: Dict[str, Any] = {
            "bbox": resized_bbox
        }
        if is_tram:
            scale: float = self._scale
            proxies: TramPositionalProxies | None = state.proxies
            if proxies is not None:
                start_resized: PlanarPosition = (proxies.proj_span_start[0] * scale,
                                                 proxies.proj_span_start[1] * scale)
                center_resized: PlanarPosition = (proxies.proj_span_center[0] * scale,
                                                  proxies.proj_span_center[1] * scale)
                end_resized: PlanarPosition = (proxies.proj_span_end[0] * scale,
                                               proxies.proj_span_end[1] * scale)
                proxies_resized: TramPositionalProxies = TramPositionalProxies(
                    proj_span_start=start_resized,
                    proj_span_center=center_resized,
                    proj_span_end=end_resized
                )
                fields_to_update["proxies"] = proxies_resized
        new_state: EnhancedTrackState = state.model_copy(update=fields_to_update)
        return new_state


    def _resize_coords_in_enhanced_track(self, track: EnhancedTrackWithHistory) -> EnhancedTrackWithHistory:
        resized_states: List[EnhancedTrackState] = [
            self._resize_enhanced_track_state(state)
            for state in track.history
        ]
        # new_track: EnhancedTrackWithHistory = EnhancedTrackWithHistory(track_id=track.track_id,
        #                                                                class_id=track.class_id,
        #                                                                vehicle_type=track.vehicle_type,
        #                                                                history=resized_states)
        new_track: EnhancedTrackWithHistory = track.model_copy(
            update={"history": resized_states}
        )
        return new_track

    def _resize_coords_in_enhanced_tracks(self, tracks: List[EnhancedTrackWithHistory]) -> List[EnhancedTrackWithHistory]:
        return [self._resize_coords_in_enhanced_track(track) for track in tracks]

    def _draw_enhanced_track_state(self, img: NDArray, state: EnhancedTrackState,
                                   *, track_id: str, class_id: int):
        # colours for the track id annotation
        colors_for_track: TrackColourPaletteItem = self._get_colors_for_track(track_id)
        # colours for the bbox border and class id annotation
        bbox_colors: BboxColours = CLASS_COLOURS[class_id]

        _draw_enhanced_track_state(img, state, track_id=track_id, class_id=class_id,
                                   bbox_colors=bbox_colors,
                                   trackid_bg_color=colors_for_track.trackid_bg_color,
                                   trackid_text_color=colors_for_track.trackid_text_color,
                                   config=self.config.track_state)

    def process_frame_old(self, frame: Frame, tracks: List[EnhancedTrackWithHistory]) -> NDArray[np.uint8]:
        src_img: NDArray = frame.image.copy()
        canvas: NDArray = src_img.copy()
        canvas: NDArray = self._transform_canvas(canvas)

        # draw the regions of interest for the detectors
        self._draw_all_roi(canvas)
        # draw rail corridors
        self._draw_rail_corridors(canvas)

        # resize all coordinates in tracks (if _transform_canvas has resized the image)
        # TODO: vectorise (straightforward in code, but very inefficient computationally)
        resized_tracks: List[EnhancedTrackWithHistory] = self._resize_coords_in_enhanced_tracks(tracks)
        for track in resized_tracks:  # type: EnhancedTrackWithHistory
            # Workaround for now: Instead of overloading _draw_track and _draw_track_state,
            # convert enhanced track to the old type of track and pass to the old functions.
            nonenhanced_track: TrackWithHistory = _track_from_enhanced(track)

            # draw the track as a line and markers
            self._draw_track_old(canvas, nonenhanced_track)
            # draw the bounding box and info (class id, track id) for the last state
            self._draw_enhanced_track_state(canvas, track.history[-1],
                                            track_id=track.track_id,
                                            class_id=track.class_id)

        # draw frame info (frame id, timestamp)
        self._draw_frame_info(canvas, frame_id=frame.frame_id,
                              timestamp=frame.timestamp)

        return canvas