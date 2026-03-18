from typing import Generator, Self
from pathlib import Path
from datetime import datetime, timedelta
from abc import ABC, abstractmethod
from fractions import Fraction
from io import BytesIO

import av
from av import VideoStream
from av.container import InputContainer
from av.video.frame import VideoFrame
from PIL import Image

from src.v1_1.models import Frame
from src.v1_1.utils import get_file_creation_time, get_uuid

class FrameResizer:

    """
    On calling the instance's `resize()` method, resizes a PyAV video frame to the height
    specified when initialising the instance.

    The target width is set so that the original aspect ratio is preserved.

    After the stream has been initialised, call the instance's `init()` method,
    providing the PyAV video stream as the argument, to compute the target height
    from the stream's aspect ratio (this is done once so that the computation
    does not have to be performed again for every frame).
    """

    def __init__(self, target_h: int | None):
        self.to_resize: bool = target_h is not None
        self.target_h: int | None = target_h
        self.target_w: int | None = None

    def init(self, stream: VideoStream) -> Self:
        # If self.to_resize is True, set the target width based on the stream's aspect ratio,
        # preserving it in target images.
        if self.to_resize:
            aspect_ratio: Fraction = Fraction(stream.codec_context.width, stream.codec_context.height)
            self.target_h: int = round(self.target_h * aspect_ratio)
        return self

    def reset(self) -> None:
        # Reset the target width.
        self.target_h = None

    def resize(self, frame: VideoFrame) -> VideoFrame:
        if self.to_resize:
            resized: VideoFrame = frame.reformat(width=self.target_w,
                                                 height=self.target_h)
            return resized
        # if the to_resize flag is set to False, simply return the original frame
        return frame


class BaseFrameStreamer(ABC):

    def __init__(self,
                 video_resource_id: str,
                 *, camera_id: str,
                 video_track_idx: int = 0,
                 target_height: int = None,
                 target_encoding: str = "PNG"):
        self._camera_id: str = camera_id
        # The resource identifier for the video (URL, file path)
        self._video_resource_id: str = video_resource_id
        self._video_track_idx: int = video_track_idx
        # Target image height; set to None to disable resizing.
        self._target_img_height: int | None = target_height
        self._target_encoding: str = target_encoding
        # Video container; initialised on entering the runtime context, closed and cleared on exiting
        self._container: InputContainer | None = None
        self._stream: VideoStream | None = None
        self._frame_resizer: FrameResizer = FrameResizer(self._target_img_height)
        # Video start time; initialised on entering the runtime context, cleared on exiting
        self._video_start_time: datetime = datetime.now()

    def __enter__(self) -> Self:
        self._container = av.open(self._video_resource_id) # type: InputContainer
        self._stream = self._container.streams.video[self._video_track_idx]
        self._video_start_time = self._get_video_start_time()
        self._frame_resizer.init(self._stream)
        return self

    def __iter__(self) -> Generator[Frame]:
        for frame in self._container.decode(self._stream):  # type: VideoFrame
            video_frame: VideoFrame = frame
            frame: Frame = self._build_frame(video_frame)
            yield frame

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._container is not None:
            self._container.close()
            self._video_start_time = None
            self._frame_resizer.reset()

    def _compute_frame_timestamp(self, frame: VideoFrame) -> datetime:
        # Return the frame's presentation time (as an instance of datetime)
        # based on its PTS and the stream's start time.
        pts: int = frame.pts
        timebase: Fraction = self._stream.time_base
        pts_sec: float = float(pts * timebase)
        timestamp: datetime = self._video_start_time + timedelta(seconds=pts_sec)
        return timestamp

    def _get_image_bytes(self, frame: VideoFrame) -> bytes:
        with frame.to_image() as pil_img, BytesIO() as bytes_out:  # type: Image, BytesIO
            pil_img.save(fp=bytes_out, format=self._target_encoding)
            img_bytes: bytes = bytes_out.getvalue()
        return img_bytes

    def _build_frame(self, input_frame_obj: VideoFrame) -> Frame:
        timestamp: datetime = self._compute_frame_timestamp(input_frame_obj)
        resized_frame_obj: VideoFrame = self._frame_resizer.resize(input_frame_obj)
        image: bytes = self._get_image_bytes(resized_frame_obj)
        frame_id: str = get_uuid()
        frame: Frame = Frame(frame_id=frame_id,
                             camera_id=self._camera_id,
                             image=image,
                             timestamp=timestamp)
        return frame

    @abstractmethod
    def _get_video_start_time(self) -> datetime:
        pass

class FileFrameStreamer(BaseFrameStreamer):

    def __init__(self, *args, video_resource_id: str, **kwargs):
        self._validate_file_path(video_resource_id)
        super().__init__(video_resource_id, *args, **kwargs)

    @staticmethod
    def _validate_file_path(video_resource_id: str):
        video_path: Path = Path(video_resource_id)
        if not video_path.exists():
            raise ValueError(f"Video file does not exist: {video_path}")
        if not video_path.is_file():
            raise ValueError(f"Not a file: {video_path}")

    def _get_video_start_time(self) -> datetime:
        # Store the file's birth time as the start time.
        return get_file_creation_time(self._video_resource_id)


def _sandbox_1():
    filepath: str = "REDACTED/dataset/portion_20251201_fps5/20251201_1300.mp4"
    # file_url: str = filepath.as_uri()
    with FileFrameStreamer(video_resource_id=filepath,
                           video_track_idx=0,
                           camera_id="cam_1",
                           target_height=320) as streamer: # type: FileFrameStreamer
        for idx, frame in enumerate(streamer): # type: int, Frame
            print(f"frame_id: {frame.frame_id} | timestamp: {frame.timestamp.isoformat()}")
            if idx == 5:
                break

if __name__ == "__main__":
    _sandbox_1()