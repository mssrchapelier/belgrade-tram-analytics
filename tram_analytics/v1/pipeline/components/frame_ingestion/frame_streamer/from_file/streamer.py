from typing import Tuple, Iterator, override
from datetime import datetime
from pathlib import Path

from common.utils.fileops_utils import get_file_creation_time
from common.utils.misc_utils import is_url
from common.utils.time_utils import get_datetime_utc
from tram_analytics.v1.models.components.frame_ingestion import Frame
from tram_analytics.v1.pipeline.components.frame_ingestion.frame_streamer.base.streamer import BaseFrameStreamer
from tram_analytics.v1.pipeline.components.frame_ingestion.frame_streamer.from_file.config import \
    EnhancedFrameStreamerConfig


class EnhancedFrameStreamer(BaseFrameStreamer):

    # TODO: merge with the base class

    def __init__(self, config: EnhancedFrameStreamerConfig) -> None:
        # self._validate_file_path(config.video_resource_id)
        self._video_start_manual_ts: datetime | None = config.video_start_manual_ts
        self._loop_video: bool = config.loop_video
        self._frame_range: Tuple[int, int] | None = config.frame_range
        super().__init__(config.video_resource_id,
                         camera_id=config.camera_id,
                         video_track_idx=config.video_track_idx,
                         target_height=config.target_height)

    def __len__(self) -> int:
        if self._context is None:
            raise RuntimeError("This streamer's context is not defined (runtime not initialised?)")
        return self._context.stream.frames

    def _reset_for_looping(self):
        if self._context is None:
            raise RuntimeError("This streamer's context is not defined (runtime not initialised?)")
        self._context.container.seek(0)
        self._context.stream.codec_context.flush_buffers()

    def __iter__(self) -> Iterator[Frame]:
        """
        Iterates over frames in the video (inside `frame_range` if specified).
        If the `loop_video` flag has been set to `True`, then, after the last frame of the video
        (if `frame_range` is `None`) or after the specified frame range has been exhausted (otherwise),
        resets the container and codec context and repeats indefinitely.
        """
        while True:
            stream_iterator: Iterator[Frame] = self._get_stream_iterator(self._frame_range)
            for frame in stream_iterator: # type: Frame
                yield frame
            if not self._loop_video:
                return
            self._reset_for_looping()

    @staticmethod
    def _validate_file_path(video_resource_id: str):
        video_path: Path = Path(video_resource_id)
        if not video_path.exists():
            raise ValueError(f"Video file does not exist: {video_path}")
        if not video_path.is_file():
            raise ValueError(f"Not a file: {video_path}")

    @override
    def _get_video_start_time(self) -> datetime:
        # for a manually set video start time in config: return it
        if self._video_start_manual_ts is not None:
            return self._video_start_manual_ts
        # URL --> current time
        # TODO: also check whether this is a file URI
        if is_url(self._video_resource_id):
            return get_datetime_utc()
        # file --> the file's birth time
        return get_file_creation_time(self._video_resource_id)
