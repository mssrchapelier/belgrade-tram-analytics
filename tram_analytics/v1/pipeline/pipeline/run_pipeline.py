from typing import Tuple, List
from pathlib import Path

from pydantic_yaml import parse_yaml_file_as

from common.settings.cpu_settings import configure_cpu_inference_runtime

configure_cpu_inference_runtime()

from tram_analytics.v1.pipeline.pipeline.artefacts_streaming.pipeline import ArtefactsStreamingPipeline
from tram_analytics.v1.pipeline.pipeline.video_writer.pipeline import VideoWriterPipeline
from tram_analytics.v1.pipeline.pipeline.image_streaming.pipeline import ImageStreamingPipeline
from tram_analytics.v1.models.pipeline_artefacts import PipelineArtefacts
from tram_analytics.v1.pipeline.pipeline.video_writer.config import VideoWriterPipelineConfig
from tram_analytics.v1.pipeline.pipeline.image_streaming.config import ImageStreamingPipelineConfig
from tram_analytics.v1.pipeline.pipeline.artefacts_streaming.config import ArtefactsStreamingPipelineConfig
from tram_analytics.v1.models.components.vehicle_info import VehicleInfo

def run_video_writer_pipeline(config_path: str | Path) -> None:
    pipeline_config: VideoWriterPipelineConfig = parse_yaml_file_as(
        VideoWriterPipelineConfig, config_path
    )
    pipeline: VideoWriterPipeline = VideoWriterPipeline(pipeline_config)
    pipeline.run()

def run_image_streaming_pipeline(config_path: str | Path) -> None:
    from numpy import uint8
    from numpy.typing import NDArray

    pipeline_config: ImageStreamingPipelineConfig = parse_yaml_file_as(
        ImageStreamingPipelineConfig, config_path
    )
    pipeline: ImageStreamingPipeline = ImageStreamingPipeline(pipeline_config)

    for img in pipeline: # type: NDArray[uint8]
        print(img.shape)

def _speed_ms_to_kmh(speed_ms: float) -> float:
    return speed_ms * 3.6

def _print_speeds_single_vehicle(vehicle_info: VehicleInfo) -> str:
    # vehicle_id | raw: N.N kmh | smoothed: N.N kmh
    vehicle_id: str = vehicle_info.vehicle_id

    speed_raw_ms: float | None = vehicle_info.speeds.raw
    speed_raw_kmh: float | None = _speed_ms_to_kmh(speed_raw_ms) if speed_raw_ms is not None else None
    speed_raw_str: str = f"{speed_raw_kmh:.1f}" if speed_raw_kmh is not None else "n/a"

    speed_smoothed_ms: float | None = vehicle_info.speeds.smoothed
    speed_smoothed_kmh: float | None = _speed_ms_to_kmh(speed_smoothed_ms) if speed_smoothed_ms is not None else None
    speed_smoothed_str: str = f"{speed_smoothed_kmh:.1f}" if speed_smoothed_kmh is not None else "n/a"

    result: str = f"{vehicle_id[:6]} | raw: {speed_raw_str} kmh | smoothed: {speed_smoothed_str} kmh"
    return result

def _print_speeds(artefacts: PipelineArtefacts) -> str:
    lines: List[str] = [
        _print_speeds_single_vehicle(vehicle_info)
        for vehicle_info in artefacts.vehicles_info
    ]
    return "\n".join(lines)

def run_artefacts_streaming_pipeline(config_path: str | Path) -> None:
    from numpy import uint8
    from numpy.typing import NDArray

    pipeline_config: ArtefactsStreamingPipelineConfig = parse_yaml_file_as(
        ArtefactsStreamingPipelineConfig, config_path
    )
    pipeline: ArtefactsStreamingPipeline = ArtefactsStreamingPipeline(pipeline_config)

    for idx, (img, artefacts) in enumerate(pipeline):  # type: int, Tuple[NDArray[uint8], PipelineArtefacts]
        # print(img.shape)
        print(artefacts.model_dump_json(indent=2, ensure_ascii=True))
        # print(f"--- {idx} ---")
        # print(_print_speeds(artefacts))
