from datetime import datetime
from typing import Tuple

from pydantic import NonNegativeInt, field_validator

from tram_analytics.v1.pipeline.components.frame_ingestion.frame_streamer.base.config import BaseFrameStreamerConfig


class EnhancedFrameStreamerConfig(BaseFrameStreamerConfig):
    # TODO: merge with the base config

    # The manually set timestamp for the first frame in the video (ISO string)
    video_start_manual_ts: datetime | None = None
    # Whether to loop the video indefinitely
    loop_video: bool = False

    # Range (inclusive) of frames to process by the pipeline. The index of the first frame is 0.
    frame_range: Tuple[NonNegativeInt, NonNegativeInt] | None = None

    @field_validator("frame_range", mode="after")
    @classmethod
    def _validate_frame_range(
            cls, frame_range: Tuple[NonNegativeInt, NonNegativeInt] | None
    ) -> Tuple[NonNegativeInt, NonNegativeInt] | None:
        if frame_range is not None:
            start, end = frame_range # type: NonNegativeInt, NonNegativeInt
            if not start <= end:
                raise ValueError("Invalid frame_range: start must be less than or equal to end")
        return frame_range
