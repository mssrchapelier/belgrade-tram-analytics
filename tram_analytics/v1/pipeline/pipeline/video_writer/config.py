from typing import Literal

from tram_analytics.v1.pipeline.pipeline.base.config import BasePipelineConfig
from tram_analytics.v1.pipeline.pipeline.output_type import PipelineOutputType


class VideoWriterPipelineConfig(BasePipelineConfig):
    out_to: Literal[PipelineOutputType.FILE] = PipelineOutputType.FILE
    out_video_path: str