from datetime import datetime

import numpy as np
from numpy._typing import NDArray
from pydantic import BaseModel


class FrameMetadata(BaseModel):
    frame_id: str
    camera_id: str
    timestamp: datetime


class Frame(FrameMetadata):
    # TODO: Change to include FrameMetadata as a field

    model_config = {
        "arbitrary_types_allowed": True
    }

    image: NDArray[np.uint8] # BGR 3-dimensional array
