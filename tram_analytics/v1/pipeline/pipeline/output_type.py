from enum import Enum

class PipelineOutputType(str, Enum):
    # annotated image
    IMG_STREAM = "stream"
    # annotated image and artefacts
    ARTEFACTS_STREAM = "artefacts_stream"
    # video file
    FILE = "file"
