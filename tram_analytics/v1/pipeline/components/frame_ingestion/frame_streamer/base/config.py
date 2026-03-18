from pydantic import BaseModel, NonNegativeInt


class BaseFrameStreamerConfig(BaseModel):
    # The resource identifier for the video (URL, file path)
    video_resource_id: str
    camera_id: str
    # The index of the video track to be decoded
    video_track_idx: NonNegativeInt = 0
    # Target image height; set to None to disable resizing
    target_height: NonNegativeInt | None = None
