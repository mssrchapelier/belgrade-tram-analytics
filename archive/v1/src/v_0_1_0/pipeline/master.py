from typing import Dict, List, Tuple, Set

from tqdm.auto import tqdm
from tqdm import tqdm as TqdmObject

from paths import BASE_DIR
from archive.v1.src.models.models import (
    Track, TrackState_Old
)
from tram_analytics.v1.models.components.tracking import DetectionToTrackState
from tram_analytics.v1.models.components.detection import Detection
from tram_analytics.v1.models.components.frame_ingestion import Frame
from tram_analytics.v1.pipeline.components.frame_ingestion.frame_streamer.from_file.streamer import FileFrameStreamer
from tram_analytics.v1.pipeline.components.detection.detection import DetectionService
from tram_analytics.v1.pipeline.components.detection.detection_config import DetectionServiceConfig, DetectorConfig
from tram_analytics.v1.pipeline.components.tracking.tracking import SortWrapper, TrackerStepOutput_Old
from tram_analytics.v1.pipeline.components.tracking.settings import SingleClassSortParams
from archive.v1.src.v_0_2_0.pipeline.components.visualizer_input_builder_v2 import (
    VisualizerInputBuilderV2, EnhancedTrackWithHistory
)
from archive.v1.src.v_0_2_0.visualizer.visualizer import VisualizerV2
from tram_analytics.v1.pipeline.pipeline.video_writer.video_writer import FileVideoWriter
from archive.v1.src.v_0_2_0.pipeline.components.analytics.scene_geometry.scene_geometry import SceneGeometryConfig
from archive.v1.src.v_0_2_0.pipeline.components.analytics.analytics_postprocessor_old import (
    AnalyticsPostprocessor, AnalyticsPostprocessorInput, AnalyticsPostprocessorOutput
)


def _get_frame_txt_output(
        frame_idx: int, frame: Frame, dets: List[Detection], tracker_output: TrackerStepOutput_Old
) -> str:
    out_str: str = f"------- FRAME {frame_idx:04d} -------\n\n"
    out_str += f"frame_id {frame.frame_id[:6]} | camera_id {frame.camera_id} | timestamp: {frame.timestamp.isoformat()}\n\n"

    out_str += "--- DETECTIONS ---\n\n"
    for det in dets:  # type: Detection
        out_str += f"detection_id {det.detection_id[:6]} | class_id {det.raw_detection.class_id} | conf {det.raw_detection.confidence:.2f}\n"
        out_str += " | ".join(
            "{: >8.2f}".format(getattr(det.raw_detection.bbox, attr_name)) for attr_name in ["x1", "y1", "x2", "y2"]
        ) + "\n\n"

    out_str += "--- TRACKER OUTPUT ---\n\n"
    out_str += "New tracks:\n\n"
    for track in tracker_output.new_tracks: # type: Track
        out_str += (f"track_id {track.track_id[:6]}"
                    f" | camera_id {track.camera_id: >10}"
                    f" | class_id {track.class_id}\n")
    out_str += "\n\nTrack states:\n\n"
    for state in tracker_output.track_states: # type: TrackState_Old
        out_str += (f"track_state_id {state.track_state_id[:6]}"
                    f" | track_id {state.track_id[:6]}"
                    f" | frame_id {state.track_id[:6]}"
                    f" | is_confirmed_track {str(state.is_confirmed_track)}\n")
        out_str += " | ".join(
            "{: >8.2f}".format(getattr(state.bbox, attr_name)) for attr_name in ["x1", "y1", "x2", "y2"]
        ) + "\n"
    out_str += "\n\nTrack state to detection mappings:\n\n"
    for mapping in tracker_output.track_state_to_detection_mappings: # type: DetectionToTrackState
        out_str += f"track_state_id {mapping.track_state_id[:6]} | detection_id {mapping.detection_id[:6]}\n"
    return out_str

def _update_registered_tracks(registered_tracks_by_id: Dict[str, Track],
                              tracker_output: TrackerStepOutput_Old):
    registered_tracks_by_id.update({
        track.track_id: track
        for track in tracker_output.new_tracks  # type: Track
    })
    dead_track_ids: Set[str] = set.difference(
        set(registered_tracks_by_id.keys()),
        set(state.track_id for state in tracker_output.track_states)
    )
    # remove dead track ids
    map(lambda track: registered_tracks_by_id.pop(track), dead_track_ids)

def run():
    from io import TextIOWrapper
    from datetime import datetime
    from pathlib import Path

    from numpy.typing import NDArray
    from pydantic_yaml import parse_yaml_file_as

    video_name: str = "VIDEO_NAME"
    video_path: str = f"REDACTED/{video_name}.mp4"
    video_start_ts: datetime = datetime.fromisoformat("2025-11-26T13:54:02+00:00")
    frame_range: Tuple[int, int] | None = None

    # TODO: if the frequency at which output frames are written depends on the input frame rate,
    #  out_fps should be computed using it (perhaps add reporting the input frame rate from the frame producer)
    out_fps: float = 5.0

    camera_id: str = "cam1"
    src_img_size: Tuple[int, int] = (568, 320)
    expected_img_shape: Tuple[int, int, int] = (src_img_size[1], src_img_size[0], 3)

    tracker_params_per_class: Dict[int, SingleClassSortParams] = {
        0: SingleClassSortParams(max_age=3, min_hits=2),
        2: SingleClassSortParams(max_age=3, min_hits=2)
    }

    visualizer_config_path: str = str(BASE_DIR / "src/v1/visualiser/visualiser.yaml")
    track_color_config_path: str = str(BASE_DIR / "src/v1/pipeline/components/colour_palette/colours.yaml")
    detection_config_path: str = str(BASE_DIR / "src/v1/pipeline/components/detection/detection.yaml")
    scene_geometry_config_path: str = str(BASE_DIR / "src/v1/pipeline/components/analytics/scene_geometry/scene_geometry.yaml")
    out_txt_path: str = f"REDACTED/{video_name}.txt"
    out_video_path: str = f"REDACTED/{video_name}.mp4"

    detection_config: DetectionServiceConfig = parse_yaml_file_as(
        DetectionServiceConfig, detection_config_path
    )
    # detection ID -> ROI
    roi_map: Dict[str, List[Tuple[float, float]]] = {
        detector_config.detector_id: detector_config.roi.coords
        for detector_config in detection_config.detectors # type: DetectorConfig
    }
    scene_geometry_config: SceneGeometryConfig = parse_yaml_file_as(
        SceneGeometryConfig, scene_geometry_config_path
    )

    frame_producer: FileFrameStreamer = FileFrameStreamer(video_resource_id=video_path,
                                                          camera_id=camera_id,
                                                          video_start_manual_ts=video_start_ts)
    detection_service: DetectionService = DetectionService.from_config(detection_config)
    tracker: SortWrapper = SortWrapper(camera_id=camera_id,
                                       class_params=tracker_params_per_class)
    # visualizer_input_builder: VisualizerInputBuilder = VisualizerInputBuilder()
    visualizer_input_builder: VisualizerInputBuilderV2 = VisualizerInputBuilderV2()
    # visualiser: Visualizer = Visualizer(visualizer_config_path, track_color_config_path,
    #                                     src_img_size=src_img_size,
    #                                     roi_map=roi_map)
    visualizer: VisualizerV2 = VisualizerV2(visualizer_config_path, track_color_config_path,
                                            src_img_size=src_img_size,
                                            roi_map=roi_map,
                                            scene_geometry_config=scene_geometry_config)
    video_writer: FileVideoWriter = FileVideoWriter(out_video_path, fps=out_fps)

    analytics_postprocessor: AnalyticsPostprocessor = AnalyticsPostprocessor(scene_geometry_config)

    # out_dir_path: Path = Path(out_img_dir)
    # out_dir_path.mkdir(parents=True)
    Path(out_video_path).parent.mkdir(parents=True, exist_ok=True)

    with (
            frame_producer, # type: FileFrameStreamer
            detection_service, # type: DetectionService
            open(out_txt_path, "w", encoding="utf8") as txt_out, # type: TextIOWrapper
            video_writer
    ):

        total_frames: int = (
            frame_range[1] - frame_range[0] if frame_range is not None
            else len(frame_producer)
        )
        pbar: TqdmObject[Frame] = tqdm(total=total_frames, desc="Processing frames...")

        # track ID -> Track
        registered_tracks_by_id: Dict[str, Track] = dict()

        for idx, frame in enumerate(frame_producer): # type: int, Frame

            if frame_range is not None:
                if idx < frame_range[0]:
                    if idx % 10 == 0:
                        print(f"frame {idx:04d} skipped")
                    continue
                if idx >= frame_range[1]:
                    break

            if frame.image.shape != expected_img_shape:
                raise RuntimeError("Got frame {} of unexpected size: expected {}, got {}".format(
                    idx, str(expected_img_shape), str(frame.image.shape)
                ))

            dets: List[Detection] = detection_service.detect(frame)

            tracker_output: TrackerStepOutput_Old = tracker.update_old(
                dets, frame_id=frame.frame_id
            )
            # add new tracks, remove old ones
            _update_registered_tracks(registered_tracks_by_id, tracker_output)

            # for trams, compute rail corridor assignments, proxy points
            analytics_input: AnalyticsPostprocessorInput = AnalyticsPostprocessorInput(
                frame=frame, detections=dets, tracking_results=tracker_output,
                tracks=list(registered_tracks_by_id.values())
            )
            analytics_output: AnalyticsPostprocessorOutput = analytics_postprocessor.process_frame_outputs(analytics_input)

            visualizer_input: List[EnhancedTrackWithHistory] = visualizer_input_builder.update(
                tracker_output, analytics_output
            )

            dest_img: NDArray = visualizer.process_frame_old(frame, visualizer_input)
            video_writer.write_frame(dest_img)

            txt_str: str = _get_frame_txt_output(idx, frame, dets, tracker_output)
            txt_out.write(txt_str)

            # time_total: float = ts_end_txtwrite - ts_start_det

            # print(f"frame: {idx:04d} | det: {time_det:.3f} s"
            #       + f"| track: {time_track:.3f}"
            #       + f" | imgproc: {time_imgproc:.3f} s"
                  # + f" | imgwrite: {time_imgwrite:.3f} s"
                  # + f" | txtwrite: {time_txtwrite:.3f} s"
                  # + f" | total: {time_total:.3f} s")
            # print(f"frame {idx:04d} processed")

            pbar.update(1)

        pbar.close()

if __name__ == "__main__":
    run()