from typing import Literal

from tram_analytics.v1.pipeline.pipeline.base.config import BasePipelineConfig
from tram_analytics.v1.pipeline.pipeline.output_type import PipelineOutputType


class ImageStreamingPipelineConfig(BasePipelineConfig):
    out_to: Literal[PipelineOutputType.IMG_STREAM] = PipelineOutputType.IMG_STREAM
