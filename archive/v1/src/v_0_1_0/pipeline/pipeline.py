__version__ = "0.1.0"

from typing import Self, Tuple, Dict, List, Set, Literal, Annotated, TypeAlias, Iterator
from abc import ABC, abstractmethod
from enum import Enum
from pathlib import Path
from warnings import deprecated

from pydantic import BaseModel, Field
from pydantic_yaml import parse_yaml_file_as
from numpy.typing import NDArray
from tqdm.auto import tqdm

from archive.v1.src.models.models import (
    Track, TrackerStepOutput_Old
)
from tram_analytics.v1.models.components.detection import Detection
from tram_analytics.v1.models.components.frame_ingestion import FrameMetadata, Frame
from tram_analytics.v1.pipeline.components.frame_ingestion.frame_streamer.from_file.streamer import FileFrameStreamer
from tram_analytics.v1.pipeline.components.frame_ingestion.frame_streamer.from_file.config import FileFrameStreamerConfig
from tram_analytics.v1.pipeline.components.detection.detection import DetectionService
from tram_analytics.v1.pipeline.components.detection.detection_config import DetectionServiceConfig
from tram_analytics.v1.pipeline.components.tracking.tracking import SortWrapper
from tram_analytics.v1.pipeline.components.tracking.settings import SingleClassSortParams
from archive.v1.src.v_0_2_0.pipeline.components.visualizer_input_builder_v2 import VisualizerInputBuilderV2, EnhancedTrackWithHistory
from archive.v1.src.v_0_2_0.visualizer.visualizer import VisualizerV2
from tram_analytics.v1.pipeline.pipeline.video_writer.video_writer import FileVideoWriter
from archive.v1.src.v_0_2_0.pipeline.components.analytics.analytics_postprocessor_old import (
    AnalyticsPostprocessor, AnalyticsPostprocessorInput, AnalyticsPostprocessorOutput, SceneGeometryConfig
)

TRACKER_PARAMS: Dict[int, SingleClassSortParams] = {
    0: SingleClassSortParams(max_age=3, min_hits=2),
    2: SingleClassSortParams(max_age=3, min_hits=2)
}

class ConfigPaths(BaseModel):
    frame_ingestion: str
    visualizer: str
    track_colors: str
    detection: str
    scene_geometry: str

class BasePipelineConfig(BaseModel):
    out_fps: float
    progress_bar: bool = False

    # TODO: Individual configs should be initialised as fields of this model.
    # TODO: Check that `progress_bar` is set to `False` if the frame producer is set to loop the video.
    #   Otherwise, it must be changed to also report the frame's index in the file
    #   for the progress bar to display sensible values.

    config_paths: ConfigPaths

class PipelineOutputType(str, Enum):
    # annotated image
    IMG_STREAM = "stream"
    FILE = "file"

class ImageStreamingPipelineConfig(BasePipelineConfig):
    out_to: Literal[PipelineOutputType.IMG_STREAM] = PipelineOutputType.IMG_STREAM

class VideoWriterPipelineConfig(BasePipelineConfig):
    out_to: Literal[PipelineOutputType.FILE] = PipelineOutputType.FILE

PipelineConfig: TypeAlias = Annotated[
    ImageStreamingPipelineConfig | VideoWriterPipelineConfig,
    Field(discriminator="out_to")
]

def _extract_frame_metadata(frame: Frame) -> FrameMetadata:
    return FrameMetadata(**frame.model_dump(exclude={"image"}))

@deprecated("Deprecated, use PipelineArtefacts instead")
class PipelineArtefacts_Old(BaseModel):
    frame_metadata: FrameMetadata
    detection: List[Detection]
    tracks: List[Track]
    track_states: TrackerStepOutput_Old
    analytics: AnalyticsPostprocessorOutput

@deprecated("Deprecated, use BasePipeline instead")
class BasePipeline_Old(ABC):

    """
    Outputs annotated images only.
    """

    def __init__(self, config: BasePipelineConfig):
        self._config: BasePipelineConfig = config
        self._init_configs(config.config_paths)
        self._runtime_initialized: bool = False

    def _init_configs(self, paths: ConfigPaths) -> None:
        self._frame_ingestion_config: FileFrameStreamerConfig = parse_yaml_file_as(
            FileFrameStreamerConfig, paths.frame_ingestion
        )
        self._detection_config: DetectionServiceConfig = parse_yaml_file_as(
            DetectionServiceConfig, paths.detection
        )
        self._scene_geometry_config: SceneGeometryConfig = parse_yaml_file_as(
            SceneGeometryConfig, paths.scene_geometry
        )

    @abstractmethod
    def _init_runtime(self) -> None:
        # set when processing the first frame
        self._expected_img_shape: Tuple[int, int, int] | None = None

        self._frame_producer: FileFrameStreamer = FileFrameStreamer(
            self._frame_ingestion_config
        )

        print("Initialising detection service ...")
        self._detection_service: DetectionService = DetectionService.from_config(
            self._detection_config
        )
        print("Detection service initialised")

        # detection ID -> ROI
        self._roi_map: Dict[str, List[Tuple[float, float]]] = {
            detector_config.detector_id: detector_config.roi.coords
            for detector_config in self._detection_config.detectors
        }
        self._tracker: SortWrapper = SortWrapper(camera_id=self._frame_ingestion_config.camera_id,
                                                 class_params=TRACKER_PARAMS)
        self._analytics_postprocessor: AnalyticsPostprocessor = AnalyticsPostprocessor(
            self._scene_geometry_config
        )
        self._visualizer_input_builder: VisualizerInputBuilderV2 = VisualizerInputBuilderV2()

        self._frames_processed: int = 0

        # track ID -> Track
        self._registered_tracks_by_id: Dict[str, Track] = dict()

        self._runtime_initialized: bool = True

    @abstractmethod
    def _reset_runtime(self) -> None:
        # TODO: set fields to None
        self._runtime_initialized: bool = False

    def _update_state_from_first_frame(self, src_img_size: Tuple[int, int]) -> None:
        self._visualizer: VisualizerV2 = VisualizerV2(self._config.config_paths.visualizer,
                                                      self._config.config_paths.track_colors,
                                                      src_img_size=src_img_size,
                                                      roi_map=self._roi_map,
                                                      scene_geometry_config=self._scene_geometry_config)
        self._expected_img_shape = (src_img_size[1], src_img_size[0], 3)

    @deprecated("Deprecated (registered tracks are not meant to be maintained any more)")
    def _update_registered_tracks(self, tracker_output: TrackerStepOutput_Old):
        self._registered_tracks_by_id.update({
            track.track_id: track
            for track in tracker_output.new_tracks
        })
        dead_track_ids: Set[str] = set.difference(
            set(self._registered_tracks_by_id.keys()),
            set(state.track_id for state in tracker_output.track_states)
        )
        # remove dead track ids
        map(lambda track: self._registered_tracks_by_id.pop(track), dead_track_ids)

    @deprecated("Deprecated, use _process_frame() instead")
    def _process_frame(self, frame: Frame) -> Tuple[NDArray, PipelineArtefacts_Old]:

        if not self._runtime_initialized:
            raise RuntimeError("Called _process_frame with runtime not initialised")

        if self._frames_processed == 0:
            img_size: Tuple[int, int] = (frame.image.shape[1], frame.image.shape[0])
            self._update_state_from_first_frame(img_size)
        else:
            if frame.image.shape != self._expected_img_shape:
                raise RuntimeError("Got frame of unexpected size: expected {}, got {}".format(
                    str(self._expected_img_shape), str(frame.image.shape)
                ))

        frame_metadata: FrameMetadata = _extract_frame_metadata(frame)
        dets: List[Detection] = self._detection_service.detect(frame)
        tracker_output: TrackerStepOutput_Old = self._tracker.update_old(
            dets, frame_id=frame.frame_id
        )
        # add new tracks, remove old ones
        self._update_registered_tracks(tracker_output)
        alive_tracks: List[Track] = list(self._registered_tracks_by_id.values())
        # for trams, compute rail corridor assignments, proxy points
        analytics_input: AnalyticsPostprocessorInput = AnalyticsPostprocessorInput(
            frame=frame, detections=dets, tracking_results=tracker_output,
            tracks=alive_tracks
        )
        analytics_output: AnalyticsPostprocessorOutput = self._analytics_postprocessor.process_frame_outputs(analytics_input)
        visualizer_input: List[EnhancedTrackWithHistory] = self._visualizer_input_builder.update(
            tracker_output, analytics_output
        )
        dest_img: NDArray = self._visualizer.process_frame_old(frame, visualizer_input)

        output: PipelineArtefacts_Old = PipelineArtefacts_Old(
            frame_metadata=frame_metadata,
            detection=dets,
            tracks=alive_tracks,
            track_states=tracker_output,
            analytics=analytics_output
        )

        self._frames_processed += 1

        return dest_img, output

    def _get_pbar(self) -> tqdm:
        if not self._config.progress_bar:
            raise ValueError("Called _get_pbar with progress_bar set to False")
        if not self._runtime_initialized:
            raise RuntimeError("Called _get_pbar with runtime not initialised")
        frame_range: Tuple[int, int] | None = self._frame_ingestion_config.frame_range
        total_frames: int = (
            frame_range[1] - frame_range[0] if frame_range is not None
            else len(self._frame_producer)
        )
        pbar: tqdm = tqdm(total=total_frames, desc="Processing frames...")
        return pbar

    def _get_frame_generator(self) -> Iterator[Tuple[NDArray, PipelineArtefacts_Old]]:
        if not self._runtime_initialized:
            raise RuntimeError("Called _get_frame_generator with runtime not initialised")

        with_pbar: bool = self._config.progress_bar

        with self._frame_producer, self._detection_service:
            pbar: tqdm | None = self._get_pbar() if with_pbar else None

            for idx, frame in enumerate(self._frame_producer):  # type: int, Frame
                if with_pbar and pbar.n == pbar.total:
                    # reset the pbar (for the looping scenario)
                    pbar.reset()
                annotated_image, artefacts = self._process_frame_old(frame) # type: NDArray, PipelineArtefacts_Old
                if with_pbar:
                    pbar.update(1)
                yield annotated_image, artefacts

    def _get_frame_generator_imgonly(self) -> Iterator[NDArray]:
        for annotated_image, artefacts in self._get_frame_generator(): # type: NDArray, PipelineArtefacts_Old
            yield annotated_image

    def __enter__(self) -> Self:
        self._init_runtime()
        self._runtime_initialized = True

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._reset_runtime()
        self._runtime_initialized = False

class ImageStreamingPipeline(BasePipeline_Old):

    def _init_runtime(self) -> None:
        super()._init_runtime()

    def _reset_runtime(self) -> None:
        super()._reset_runtime()

    def __iter__(self) -> Iterator[NDArray]:
        with self:
            for out_img in self._get_frame_generator_imgonly():  # type: NDArray
                yield out_img
        print(f"Pipeline completed")


class VideoWriterPipeline(BasePipeline_Old):

    def __init__(self, config: VideoWriterPipelineConfig,
                 *, out_video_path: str):
        super().__init__(config)
        self._out_video_path: str = out_video_path

    def _init_runtime(self) -> None:
        super()._init_runtime()
        self._video_writer: FileVideoWriter = FileVideoWriter(self._out_video_path,
                                                              fps=self._config.out_fps)
        Path(self._out_video_path).parent.mkdir(parents=True, exist_ok=True)

    def _reset_runtime(self) -> None:
        self._video_writer.close_runtime()
        super()._reset_runtime()

    def run(self) -> None:
        with self:
            for out_img in self._get_frame_generator_imgonly(): # type: NDArray
                self._video_writer.write_frame(out_img)
            print(f"Pipeline completed. Video written to: {self._out_video_path}")
