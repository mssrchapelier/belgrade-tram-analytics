from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Tuple, Iterator, Self

from numpy import uint8
from numpy._typing import NDArray
from pydantic_yaml import parse_yaml_file_as
from tqdm import tqdm

from common.settings.constants import ASSETS_DIR
from common.utils.time_utils import get_datetime_utc
from common.utils.tqdm_utils import ManualTqdm
from tram_analytics.v1.models.pipeline_artefacts import PipelineArtefacts
from tram_analytics.v1.models.components.detection import Detection
from tram_analytics.v1.models.components.frame_ingestion import FrameMetadata, Frame
from tram_analytics.v1.models.components.tracking import TrackState, TrackHistory, DetectionToTrackState
from tram_analytics.v1.models.components.vehicle_info import VehicleInfo
from tram_analytics.v1.pipeline.components.detection.detection import DetectionService, build_detection_service
from tram_analytics.v1.pipeline.components.detection.detection_config import DetectionServiceConfig
from tram_analytics.v1.pipeline.components.frame_ingestion.frame_streamer.from_file.config import \
    FileFrameStreamerConfig
from tram_analytics.v1.pipeline.components.frame_ingestion.frame_streamer.from_file.streamer import \
    FileFrameStreamer
from tram_analytics.v1.pipeline.components.scene_state.config.scene_events_config import SceneEventsConfig, \
    SceneStateUpdaterConfig
from tram_analytics.v1.pipeline.components.scene_state.live_state_updater.config.converter import \
    convert_zones_config
from tram_analytics.v1.pipeline.components.scene_state.models import SceneState
from tram_analytics.v1.pipeline.components.scene_state.scene_state import SceneStateUpdater
from tram_analytics.v1.pipeline.components.track_history_updater import TrackHistoryUpdater
from tram_analytics.v1.pipeline.components.tracking.settings import TRACKER_PARAMS
from tram_analytics.v1.pipeline.components.tracking.tracking import SortWrapper
from tram_analytics.v1.pipeline.components.vehicle_info.coord_conversion.homography_config import HomographyConfig
from tram_analytics.v1.pipeline.components.vehicle_info.speeds.config import SpeedCalculatorConfig
from tram_analytics.v1.pipeline.components.vehicle_info.vehicle_info_updater import ZoneAndSpeedAssigner
from tram_analytics.v1.pipeline.components.vehicle_info.zones.zones_config import ZonesConfig
from tram_analytics.v1.pipeline.components.visualiser.config.visualiser_config import VisualiserConfig
from tram_analytics.v1.pipeline.components.visualiser.config.colour_palette import TrackColourPalette
from tram_analytics.v1.pipeline.components.visualiser.visualiser import Visualiser
from tram_analytics.v1.pipeline.pipeline.base.config import ConfigPaths, BasePipelineConfig


def _extract_frame_metadata(frame: Frame) -> FrameMetadata:
    return FrameMetadata(**frame.model_dump(exclude={"image"}))

class BasePipeline(ABC):

    """
    Outputs annotated images only.
    """

    def __init__(self, config: BasePipelineConfig):
        self._config: BasePipelineConfig = config
        self._init_configs(config.config_paths)
        self._runtime_initialized: bool = False

    def _init_configs(self, paths: ConfigPaths) -> None:
        self._frame_ingestion_config: FileFrameStreamerConfig = parse_yaml_file_as(
            FileFrameStreamerConfig, ASSETS_DIR / paths.frame_ingestion
        )
        self._detection_config: DetectionServiceConfig = parse_yaml_file_as(
            DetectionServiceConfig, ASSETS_DIR / paths.detection
        )
        self._zones_config: ZonesConfig = parse_yaml_file_as(
            ZonesConfig, ASSETS_DIR / paths.zones
        )
        self._speeds_config: SpeedCalculatorConfig = parse_yaml_file_as(
            SpeedCalculatorConfig, ASSETS_DIR / paths.speed
        )
        self._homography_config: HomographyConfig | None = parse_yaml_file_as(
            HomographyConfig, ASSETS_DIR / paths.homography
        ) if paths.homography is not None else None
        self._scene_events_config: SceneEventsConfig = parse_yaml_file_as(
            SceneEventsConfig, ASSETS_DIR / paths.scene_events
        )
        self._scene_state_updater_config: SceneStateUpdaterConfig = SceneStateUpdaterConfig(
            zones=convert_zones_config(self._zones_config),
            scene_events=self._scene_events_config
        )
        self._visualiser_config: VisualiserConfig = parse_yaml_file_as(
            VisualiserConfig, ASSETS_DIR / paths.visualiser
        )
        self._track_colour_palette: TrackColourPalette = parse_yaml_file_as(
            TrackColourPalette, ASSETS_DIR / paths.track_colours
        )

    @abstractmethod
    def _init_runtime(self) -> None:

        self._frame_producer: FileFrameStreamer = FileFrameStreamer(
            self._frame_ingestion_config
        )

        print("Initialising detection service ...")
        self._detection_service: DetectionService = build_detection_service(
            self._detection_config
        )
        print("Detection service initialised")

        # detector ID -> ROI
        self._roi_map: Dict[str, List[Tuple[float, float]]] = {
            detector_config.detector_id: detector_config.roi.coords
            for detector_config in self._detection_config.detectors
        }
        self._tracker: SortWrapper = SortWrapper(camera_id=self._frame_ingestion_config.camera_id,
                                                 class_params=TRACKER_PARAMS)
        self._track_history_updater: TrackHistoryUpdater = TrackHistoryUpdater()
        self._zone_and_speed_assigner: ZoneAndSpeedAssigner = ZoneAndSpeedAssigner(
            zones_config=self._zones_config,
            speed_config=self._speeds_config,
            homography_config=self._homography_config
        )
        self._scene_state_updater: SceneStateUpdater = SceneStateUpdater(
            camera_id=self._frame_ingestion_config.camera_id,
            config=self._scene_state_updater_config
        )

        # set when processing the first frame
        self._visualiser: Visualiser | None = None
        self._expected_img_shape: Tuple[int, int, int] | None = None

        self._frames_processed: int = 0

        self._runtime_initialized = True

    @abstractmethod
    def _reset_runtime(self) -> None:
        # TODO: set fields to None
        self._runtime_initialized = False

    def _update_state_from_first_frame_nodrawing(self, src_img_size: Tuple[int, int]) -> None:
        self._expected_img_shape = (src_img_size[1], src_img_size[0], 3)

    def _update_state_from_first_frame(self, src_img_size: Tuple[int, int]) -> None:
        self._visualiser = Visualiser(self._visualiser_config,
                                      self._track_colour_palette,
                                      src_img_size=src_img_size,
                                      roi_map=self._roi_map,
                                      zones_config=self._zones_config)
        self._expected_img_shape = (src_img_size[1], src_img_size[0], 3)

    def _process_frame(self, frame: Frame) -> Tuple[NDArray[uint8], PipelineArtefacts]:

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

        frame_processing_start_ts: datetime = get_datetime_utc()

        frame_metadata: FrameMetadata = _extract_frame_metadata(frame)
        dets: List[Detection] = self._detection_service.detect(frame)
        track_states, det_to_track_state = self._tracker.update(dets) # type: List[TrackState], List[DetectionToTrackState]
        vehicle_infos: List[VehicleInfo] = self._zone_and_speed_assigner.process_for_frame(
            states=track_states, frame_ts=frame_metadata.timestamp
        )

        scene_state: SceneState = self._scene_state_updater.update_and_get_events(
            frame_metadata, vehicle_infos
        )

        artefacts_creation_ts: datetime = get_datetime_utc()

        artefacts: PipelineArtefacts = PipelineArtefacts(
            frame_metadata=frame_metadata,
            track_states=track_states,
            vehicles_info=vehicle_infos,
            detection=dets, det_to_track_state=det_to_track_state,
            scene_events=scene_state.scene_events,
            live_state=scene_state.live_state,
            processing_start_ts=frame_processing_start_ts,
            artefacts_creation_ts=artefacts_creation_ts
        )

        self._track_history_updater.update(track_states)
        track_histories: List[TrackHistory] = self._track_history_updater.export()

        if self._visualiser is None:
            # should have already been initialised from the first frame
            raise RuntimeError("The visualiser has not been initialised")
        dest_img: NDArray[uint8] = self._visualiser.process_frame(
            frame=frame,
            track_histories=track_histories,
            vehicle_infos=vehicle_infos
        )

        self._frames_processed += 1

        return dest_img, artefacts

    def _get_pbar(self) -> ManualTqdm:
        if not self._config.progress_bar:
            raise ValueError("Called _get_pbar with progress_bar set to False")
        if not self._runtime_initialized:
            raise RuntimeError("Called _get_pbar with runtime not initialised")
        frame_range: Tuple[int, int] | None = self._frame_ingestion_config.frame_range
        total_frames: int = (
            frame_range[1] - frame_range[0] + 1 if frame_range is not None
            else len(self._frame_producer)
        )
        pbar: ManualTqdm = tqdm(total=total_frames, desc="Processing frames...")
        return pbar

    def _get_frame_generator(self) -> Iterator[
        Tuple[NDArray[uint8], PipelineArtefacts]
    ]:
        if not self._runtime_initialized:
            raise RuntimeError("Called _get_frame_generator with runtime not initialised")

        with_pbar: bool = self._config.progress_bar

        with self._frame_producer, self._detection_service:
            pbar: ManualTqdm | None = self._get_pbar() if with_pbar else None

            for idx, frame in enumerate(self._frame_producer):  # type: int, Frame
                if pbar is not None and pbar.n == pbar.total:
                    # reset the pbar (for the looping scenario)
                    pbar.reset()
                annotated_image, artefacts = self._process_frame(frame) # type: NDArray[uint8], PipelineArtefacts
                if pbar is not None:
                    pbar.update(1)
                yield annotated_image, artefacts

    def _get_frame_generator_imgonly(self) -> Iterator[NDArray[uint8]]:
        for annotated_image, artefacts in self._get_frame_generator(): # type: NDArray[uint8], PipelineArtefacts
            yield annotated_image

    def __enter__(self) -> Self:
        self._init_runtime()
        self._runtime_initialized = True
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self._reset_runtime()
        self._runtime_initialized = False

