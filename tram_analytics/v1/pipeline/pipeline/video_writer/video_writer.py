from fractions import Fraction
from typing import List, Dict, Self, Any

import av
from av import VideoStream, Packet
from av.container import OutputContainer
from av.stream import Stream
from av.video.frame import VideoFrame
from numpy import uint8, ascontiguousarray, ndarray
from numpy.typing import NDArray

CODEC: str = "libx264"
PIXEL_FORMAT: str = "yuv420p"

STREAM_OPTS: Dict[str, str] = {
    "crf": "17",  # 23
    "preset": "medium"
}

class VideoWriterRuntime:

    def __init__(self, *, width: int, height: int, container: OutputContainer, stream: VideoStream):
        self.width: int = width
        self.height: int = height
        self.container: OutputContainer = container
        self.stream: VideoStream = stream

class FileVideoWriter:

    def __init__(self, output_path: str, *, fps: float):
        self.output_path: str = output_path

        self.fps_frac: Fraction = Fraction(fps).limit_denominator(100_000)
        self.timebase: Fraction = Fraction(1, 1) / self.fps_frac

        self.codec: str = CODEC
        self.pixel_format: str = PIXEL_FORMAT
        self.stream_opts: Dict[str, Any] = STREAM_OPTS

        self._runtime: VideoWriterRuntime | None = None
        self._cur_pts: int = 0

    def close_runtime(self) -> None:
        if self._runtime is not None:
            packets: List[Packet] = self._runtime.stream.encode(None)
            for packet in packets: # type: Packet
                self._runtime.container.mux(packet)
            self._runtime.container.close()
            self._runtime = None
            self._cur_pts = 0

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close_runtime()


    @staticmethod
    def _validate_image(img: NDArray[uint8]) -> None:
        if not (isinstance(img, ndarray)
                and img.dtype == uint8
                and len(img.shape) == 3
                and img.shape[2] == 3):
            raise ValueError("img must be a NumPy array of dtype uint8 and shape (h, w, 3)")

    def _init_runtime(self, img: NDArray[uint8]) -> None:
        height: int = img.shape[0]
        width: int = img.shape[1]
        container: OutputContainer = av.open(self.output_path, mode="w")

        stream: Stream = container.add_stream(codec_name=self.codec,
                                              rate=self.fps_frac)
        assert isinstance(stream, VideoStream)
        stream.width = width
        stream.height = height
        stream.pix_fmt = self.pixel_format
        stream.codec_context.time_base = self.timebase
        stream.options = self.stream_opts

        self._runtime = VideoWriterRuntime(width=width, height=height, container=container, stream=stream)

    def _check_same_dimensions(self, img: NDArray[uint8]) -> None:
        if self._runtime is None:
            raise RuntimeError("_check_dimensions was called with this instance's runtime set to None")
        if not (img.shape[0] == self._runtime.height and img.shape[1] == self._runtime.width):
            raise ValueError(
                "All frames must have identical shape. Set on the first frame: {}. Got now: {}.".format(
                    f"({self._runtime.height}, {self._runtime.width}, 3)",
                    f"{img.shape}"
                )
            )

    def _encode_and_mux_frame(self, img: NDArray[uint8]) -> None:
        if self._runtime is None:
            raise RuntimeError("_encode_and_mux_frame was called with this instance's runtime set to None")
        img = ascontiguousarray(img)
        frame: VideoFrame = VideoFrame.from_ndarray(img, format="bgr24")
        frame = frame.reformat(format=self._runtime.stream.pix_fmt)
        frame.pts = self._cur_pts
        frame.time_base = self._runtime.stream.codec_context.time_base
        packets: List[Packet] = self._runtime.stream.encode(frame)
        for packet in packets: # type: Packet
            self._runtime.container.mux(packet)

    def write_frame(self, img: NDArray[uint8]) -> None:
        self._validate_image(img)

        if self._cur_pts == 0:
            assert self._runtime is None
            self._init_runtime(img)
        else:
            if self._runtime is None:
                raise RuntimeError(f"Called write_frame with this instance's _cur_pts greater than 0 "
                                   f"(current value: {self._cur_pts} but null _runtime")
            self._check_same_dimensions(img)

        self._encode_and_mux_frame(img)
        self._cur_pts += 1
