__version__ = "0.2.0"

from typing import Dict, Any

from pydantic_yaml import parse_yaml_file_as

from archive.v1.src.v_0_1_0.pipeline.pipeline import (
    VideoWriterPipelineConfig, VideoWriterPipeline, ImageStreamingPipelineConfig, ImageStreamingPipeline
)

def _get_common_kwargs(video_name: str) -> Dict[str, Any]:
    kwargs: Dict[str, Any] = {
        "src_video_path": f"REDACTED/short_videos/{video_name}.mp4",
        "camera_id": "cam_1",
        "video_start_ts": "2025-11-26T13:54:02+00:00",
        "frame_range": (0, 20)
    }
    return kwargs

def run_video_writer_pipeline():
    video_name: str = "REDACTED"

    config_path: str = "src/v1/pipeline/video_writer.yaml"
    dest_video_path: str = f"REDACTED/{video_name}_exp.mp4"
    common_kwargs: Dict[str, Any] = _get_common_kwargs(video_name)

    pipeline_config: VideoWriterPipelineConfig = parse_yaml_file_as(
        VideoWriterPipelineConfig, config_path
    )
    pipeline: VideoWriterPipeline = VideoWriterPipeline(
        pipeline_config, out_video_path=dest_video_path, **common_kwargs
    )
    pipeline.run()

def run_image_streaming_pipeline():
    from numpy.typing import NDArray

    video_name: str = "REDACTED"
    config_path: str = "src/v1/pipeline/image_streaming.yaml"
    common_kwargs: Dict[str, Any] = _get_common_kwargs(video_name)

    pipeline_config: ImageStreamingPipelineConfig = parse_yaml_file_as(
        ImageStreamingPipelineConfig, config_path
    )
    pipeline: ImageStreamingPipeline = ImageStreamingPipeline(pipeline_config, **common_kwargs)

    for img in pipeline: # type: NDArray
        print(img.shape)


if __name__ == "__main__":
    # run_video_writer_pipeline()
    run_image_streaming_pipeline()