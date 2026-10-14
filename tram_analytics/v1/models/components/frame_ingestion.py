from datetime import datetime

import numpy as np
from numpy._typing import NDArray
from pydantic import BaseModel, Field


class FrameMetadata(BaseModel):
    """
    Metadata for an individual frame.
    """
    frame_id: str = Field(description="The UUID of the frame.")
    camera_id: str = Field(description="The ID of the camera that produced this frame.")
    timestamp: datetime


class Frame(FrameMetadata):
    # TODO: Change to include FrameMetadata as a field

    model_config = {
        "arbitrary_types_allowed": True
    }

    image: NDArray[np.uint8] # BGR 3-dimensional array
