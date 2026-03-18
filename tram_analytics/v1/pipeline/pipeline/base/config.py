from pydantic import BaseModel

class ConfigPaths(BaseModel):
    frame_ingestion: str
    visualiser: str
    track_colours: str
    detection: str
    zones: str
    speed: str
    homography: str | None
    scene_events: str

class BasePipelineConfig(BaseModel):
    out_fps: float
    progress_bar: bool = False

    # TODO: Check that `progress_bar` is set to `False` if the frame producer is set to loop the video.
    #   Otherwise, it must be changed to also report the frame's index in the file
    #   for the progress bar to display sensible values.

    config_paths: ConfigPaths
