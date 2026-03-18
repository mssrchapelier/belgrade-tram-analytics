from typing import Tuple, List, NamedTuple, Dict, Set, TypeAlias

from numpy import uint8
from numpy.typing import NDArray

from archive.v1.src.misc.visualizer_v0 import BboxColours, CLASS_COLOURS, to_px, PixelPoint
from archive.v1.src.misc.visualizer_v0 import _draw_track, _draw_track_state
from archive.v1.src.v_0_2_0.pipeline.components.analytics.scene_geometry.scene_geometry import (
    SceneGeometryConfig, RailCorridorConfig
)
from archive.v1.src.v_0_2_0.pipeline.components.visualizer_input_builder import TrackState as LegacyTrackState
from archive.v1.src.v_0_2_0.visualizer.visualizer import (
    VisualizerV2, PROXY_POINT_SIZE, PROXY_POINT_COLOR, PROXY_POINT_THICKNESS, LINE_TYPE
)
from common.utils.custom_types import ColorTuple, PlanarPosition
from common.utils.img.cv2.pretty_put_text import anchor_line_with_bg
from tram_analytics.v1.models.common_types import VehicleType
from tram_analytics.v1.models.common_types import get_speed_unit_str, BoundingBox, convert_speed
from tram_analytics.v1.models.components.frame_ingestion import Frame
from tram_analytics.v1.models.components.tracking import TrackState, TrackHistory
from tram_analytics.v1.models.components.vehicle_info import (
    VehicleInfo, TramRefPoints, CarRefPoints, PositionContainer
)
from tram_analytics.v1.pipeline.components.vehicle_info.zones.zones_config import ZonesConfig
from tram_analytics.v1.pipeline.components.visualiser.config.visualiser_config import VisualiserConfig
from tram_analytics.v1.pipeline.components.visualiser.config.colour_palette import (
    TrackColourPalette, TrackColourPaletteItem
)
from tram_analytics.v1.pipeline.components.visualiser.config.colour_palette import TrackLineMarkerColorPalette
from tram_analytics.v1.pipeline.components.visualiser.config.visualiser_config import (
    SpeedConfig, ColorlessTextboxConfig
)

# detector id -> coords
ROIMap: TypeAlias = Dict[str, List[PlanarPosition]]

from common.utils.img.cv2.drawing import draw_cross

def _zones_config_to_scene_geometry_config(zones_cfg: ZonesConfig) -> SceneGeometryConfig:
    """
    Extracts track information from a `ZonesConfig` and transforms to an older model.
    """
    rail_corridors: List[RailCorridorConfig] = [
        RailCorridorConfig(
            corridor_id=track_config.zone_numerical_id,
            polygon=track_config.coords.polygon,
            centerline=track_config.coords.centreline
        )
        for track_config in zones_cfg.tracks.zones
    ]
    return SceneGeometryConfig(rail_corridors=rail_corridors)

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
    anchor: PixelPoint = to_px((px_bbox.x1, px_bbox.y1))
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

class Visualiser(VisualizerV2):

    # TODO: move implementation from VisualizerV2 here, adapt to use new DTOs, remove VisualizerV2

    def __init__(
            self, config: VisualiserConfig,
            track_color_config: TrackColourPalette,
            *, src_img_size: Tuple[int, int],
            roi_map: ROIMap | None = None,
            zones_config: ZonesConfig | None = None
    ) -> None:
        scene_geometry_config: SceneGeometryConfig | None = (
            _zones_config_to_scene_geometry_config(zones_config)
            if zones_config is not None
            else None
        )
        super().__init__(config,
                         track_color_config,
                         src_img_size=src_img_size,
                         roi_map=roi_map,
                         scene_geometry_config=scene_geometry_config)

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

    @staticmethod
    def _track_state_to_legacy(state: TrackState) -> LegacyTrackState:
        """
        Helper to convert to a legacy DTO used by imported functions.
        """
        return LegacyTrackState(is_matched=state.is_matched,
                                is_confirmed=state.is_confirmed_track,
                                bbox=state.bbox)

    def _scale_legacy_track_state(self, state: LegacyTrackState) -> LegacyTrackState:
        return LegacyTrackState(is_matched=state.is_matched,
                                is_confirmed=state.is_confirmed,
                                bbox=self._scale_bbox(state.bbox))

    # --- drawing ---

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
            self._get_colors_for_track(track_history.track_id).lines_markers
        )
        # convert to legacy objects because `_draw_track` expects them
        # + also scale to canvas
        history_legacyformat: List[LegacyTrackState] = [
            self._scale_legacy_track_state(
                self._track_state_to_legacy(track_state)
            )
            for track_state in track_history.history
        ]
        _draw_track(img,
                    history_legacyformat,
                    config=self.config.track,
                    color_config=color_config)

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
            for container in containers_to_draw: # type: PositionContainer
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

    def _draw_vehicle_state(
            self, img: NDArray[uint8], state_data: VehicleStateDrawingData
    ) -> None:
        # colours for the track id annotation
        colours_for_track: TrackColourPaletteItem = self._get_colors_for_track(
            state_data.vehicle_id
        )
        # colours for the bbox border and class id annotation
        bbox_colours: BboxColours = CLASS_COLOURS[
            state_data.track_state.class_id
        ]

        # conversions to legacy format
        state_legacyformat: LegacyTrackState = self._track_state_to_legacy(state_data.track_state)
        # scale to canvas
        state_legacy_scaled: LegacyTrackState = self._scale_legacy_track_state(state_legacyformat)
        # draw the bounding box, class ID, track ID
        _draw_track_state(img, state_legacy_scaled,
                          track_id=state_data.vehicle_id,
                          class_id=state_data.track_state.class_id,
                          trackid_bg_color=colours_for_track.trackid_bg_color,
                          trackid_text_color=colours_for_track.trackid_text_color,
                          bbox_colors=bbox_colours,
                          config=self.config.track_state)
        # draw reference points
        self._draw_reference_points(img, state_data.vehicle_info)
        # draw speeds
        # - value to display: smoothed
        _draw_vehicle_speed(img,
                            state_data.vehicle_info.speeds.smoothed,
                            px_bbox=self._scale_bbox(state_data.track_state.bbox),
                            config=self.config.speed,
                            bg_color=colours_for_track.trackid_bg_color,
                            text_color=colours_for_track.trackid_text_color)

    def process_frame(self,
                      *, frame: Frame,
                      track_histories: List[TrackHistory],
                      vehicle_infos: List[VehicleInfo]) -> NDArray[uint8]:
        canvas: NDArray[uint8] = frame.image.copy()
        canvas = self._transform_canvas(canvas)

        # draw the regions of interest for the detectors
        self._draw_all_roi(canvas)
        # draw rail corridors
        self._draw_rail_corridors(canvas)
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

