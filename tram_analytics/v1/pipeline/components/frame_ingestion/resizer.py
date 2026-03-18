from fractions import Fraction

from av import VideoStream
from av.video.frame import VideoFrame


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

    def __init__(self, target_h: int | None) -> None:
        if target_h is not None and target_h <= 0:
            raise ValueError("Target height must be null or a positive integer")
        self.target_h: int | None = target_h
        self.target_w: int | None = None
        self.to_resize: bool = target_h is not None

    def init(self, stream: VideoStream) -> None:
        # If self.to_resize is True, set the target width based on the stream's aspect ratio,
        # preserving it in target images.
        self.target_w = self._get_target_w(
            orig_w=stream.codec_context.width,
            orig_h=stream.codec_context.height,
            target_h=self.target_h
        )

    @staticmethod
    def _get_target_w(*, orig_w: int, orig_h: int, target_h: int | None) -> int | None:
        if target_h is None:
            return None
        aspect_ratio: Fraction = Fraction(orig_w, orig_h)
        return round(target_h * aspect_ratio)

    def reset(self) -> None:
        # Reset the target width.
        self.target_h = None

    def resize(self, frame: VideoFrame) -> VideoFrame:
        if self.to_resize:
            assert self.target_h
            if self.target_w is None:
                raise RuntimeError("The target width is null with non-null target height "
                                   "(instance not initialised?)")
            resized: VideoFrame = frame.reformat(width=self.target_w,
                                                 height=self.target_h)
            return resized
        # if the to_resize flag is set to False, simply return the original frame
        return frame


