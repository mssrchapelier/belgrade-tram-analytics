from typing import Self, Tuple, Iterator, cast, NamedTuple
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from fractions import Fraction
from pathlib import Path

import av
import cv2
import numpy as np
from av import VideoStream, VideoFrame
from av.container import InputContainer
from cv2.typing import MatLike
from numpy import uint8, uint16, float16, float32, ndarray
from numpy.typing import NDArray

from common.settings.constants import ASSETS_DIR
from common.utils.random.id_gen import get_uuid
from common.utils.misc_utils import is_url
from common.utils.fileops_utils import resolve_rel_path
from tram_analytics.v1.models.components.frame_ingestion import Frame
from tram_analytics.v1.pipeline.components.frame_ingestion.resizer import FrameResizer


class StreamerContext(NamedTuple):
    # context: initialised on entering the runtime context, closed and cleared on exiting
    container: InputContainer
    stream: VideoStream
    video_start_time: datetime

class BaseFrameStreamer(ABC):

    def __init__(self,
                 video_resource_id: str,
                 *, camera_id: str,
                 video_track_idx: int = 0,
                 target_height: int | None = None):
        self._camera_id: str = camera_id
        # The resource identifier for the video (URL, file path)
        self._video_resource_id: str = self._transform_video_resource_id(video_resource_id)
        self._video_track_idx: int = video_track_idx
        # Target image height; set to None to disable resizing.
        self._target_img_height: int | None = target_height
        self._frame_resizer: FrameResizer = FrameResizer(self._target_img_height)

        self._context: StreamerContext | None = None

    @classmethod
    def _transform_video_resource_id(cls, resource_id: str) -> str:
        # if a URL: leave as is
        if is_url(resource_id):
            return resource_id
        # if a filepath: must be relative -> convert to absolute (rel to ASSETS_DIR)
        rel_path: Path = Path(resource_id)
        abs_path: Path = resolve_rel_path(rel_path, ASSETS_DIR)
        return str(abs_path)

    def _init_runtime(self) -> None:
        container: InputContainer = av.open(self._video_resource_id)
        stream: VideoStream = container.streams.video[self._video_track_idx]
        video_start_time: datetime = self._get_video_start_time()
        self._context = StreamerContext(container, stream, video_start_time)
        self._frame_resizer.init(stream)

    def __enter__(self) -> Self:
        self._init_runtime()
        return self

    @staticmethod
    def _validate_frame_range(frame_range: Tuple[int, int] | None) -> None:
        if frame_range is None:
            return
        if len(frame_range) != 2:
            raise ValueError(f"frame_range must be of length 2, got: {str(frame_range)}")
        start, end = frame_range # type: int, int
        if not isinstance(start, int) and isinstance(end, int):
            raise ValueError(f"frame_range must contain integers, got: {str(frame_range)}")
        if not start >= 0 and end >= start:
            raise ValueError(f"frame_range must positive integers in ascending order, got: {str(frame_range)}")


    def _get_stream_iterator(self,
                             frame_range: Tuple[int, int] | None = None) -> Iterator[Frame]:
        """
        Get an iterator over the currently initialised video stream.
        If `frame_range` is not `None`, build and return frames inside the specified range of frame indices.

        NOTE: This method decodes all frames from the container's current pointer (intended to be at the beginning)
        even if `frame_range` has been specified.
        TODO: Implement seeking to the nearest keyframe earlier than the specified start frame,
          to avoid decoding unneeded frames.

        :param frame_range: The range of frame indices (both ends inclusive) inside which to build and return frames.
                            If `None`, process and return all frames.
        :return: An iterator over `Frame` instances from the stream.
        """
        self._validate_frame_range(frame_range)
        if self._context is None:
            raise RuntimeError("This streamer's context is not defined (runtime not initialised?)")
        start, end = frame_range if frame_range is not None else (None, None) # type: int | None, int | None
        for idx, frame in enumerate(
                self._context.container.decode(self._context.stream)
        ):  # type: int, VideoFrame
            video_frame: VideoFrame = frame
            if start is None or end is None or start <= idx <= end:
                frame_dto: Frame = self._build_frame(video_frame)
                yield frame_dto
            if start is not None and end is not None and idx == end:
                return

    @abstractmethod
    def __iter__(self) -> Iterator[Frame]:
        pass

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._context is not None:
            self._context.container.close()
            self._frame_resizer.reset()
            self._context = None

    def _compute_frame_timestamp(self, frame: VideoFrame) -> datetime:
        # Return the frame's presentation time (as an instance of datetime)
        # based on its PTS and the stream's start time.
        # TODO: use the current timestamp instead of calculations based on frame PTS?
        if self._context is None:
            raise RuntimeError("This streamer's context is not defined (runtime not initialised?)")
        pts: int | None = frame.pts
        if pts is None:
            raise RuntimeError("Got frame without a presentation timestamp: handling not implemented")
        timebase: Fraction | None = self._context.stream.time_base
        if timebase is None:
            raise RuntimeError("Got stream without a timebase: handling not implemented")
        pts_sec: float = float(pts * timebase)
        timestamp: datetime = self._context.video_start_time + timedelta(seconds=pts_sec)
        return timestamp

    @staticmethod
    def _get_numpy(frame: VideoFrame) -> NDArray[uint8]:
        frame_rgb: VideoFrame = frame.to_rgb()
        img_rgb: NDArray[uint8] | NDArray[uint16] | NDArray[float16] | NDArray[float32] = (
            frame_rgb.to_ndarray(channel_last=True)
        )
        if img_rgb.dtype != uint8:
            raise RuntimeError(f"Got array of type {img_rgb.dtype} after conversion; not supported")
        img_bgr: MatLike = cv2.cvtColor(img_rgb, cv2.COLOR_RGB2BGR)
        assert isinstance(img_bgr, ndarray)
        assert img_bgr.dtype == uint8
        img_bgr = cast(NDArray[uint8], img_bgr)
        # write-protect: downstream should create an actual copy
        # if it wants to mutate the image (e. g. visualisation)
        img_bgr.flags.writeable = False
        return img_bgr

    def _build_frame(self, input_frame_obj: VideoFrame) -> Frame:
        timestamp: datetime = self._compute_frame_timestamp(input_frame_obj)
        resized_frame_obj: VideoFrame = self._frame_resizer.resize(input_frame_obj)
        image: NDArray[np.uint8] = self._get_numpy(resized_frame_obj)
        frame_id: str = get_uuid()
        frame: Frame = Frame(frame_id=frame_id,
                             camera_id=self._camera_id,
                             image=image,
                             timestamp=timestamp)
        return frame

    @abstractmethod
    def _get_video_start_time(self) -> datetime:
        pass
