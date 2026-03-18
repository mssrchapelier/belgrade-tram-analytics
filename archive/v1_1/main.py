from typing import override
from asyncio import Queue
from abc import ABC, abstractmethod
from dataclasses import dataclass

import numpy as np
from numpy import uint8
from numpy.typing import NDArray

class LiveState:
    pass

@dataclass(frozen=True, slots=True, kw_only=True)
class ArtefactsForFrame(ABC):
    frame_id: str
    camera_id: str

class BaseProcessor(ABC):

    @abstractmethod
    async def start_camera(self, camera_id: str) -> None:
        pass

    @abstractmethod
    async def stop_camera(self, camera_id: str) -> None:
        pass

# --- frames ---

@dataclass(frozen=True, slots=True, kw_only=True)
class Frame(ArtefactsForFrame):
    image: NDArray[uint8]

class FrameGetter(BaseProcessor):

    @override
    async def start_camera(self, camera_id: str) -> None:
        pass

    @override
    async def stop_camera(self, camera_id: str) -> None:
        pass

    async def get_frame_by_id(self, output_obj_id: str) -> Frame:
        pass

    async def get_next_frame_id(self, camera_id: str) -> str:
        pass

# --- detections ---

@dataclass(frozen=True, slots=True, kw_only=True)
class Detections(ArtefactsForFrame):
    detections_id: str

class ObjectDetector(BaseProcessor[Detections]):

    @override
    async def start_camera(self, camera_id: str) -> None:
        pass

    @override
    async def stop_camera(self, camera_id: str) -> None:
        pass

    async def detect(self, frame_id: str) -> str:
        # return: detections_id
        pass

    @override
    async def get_detections_by_id(self, output_obj_id: str) -> Detections:
        pass

    @override
    async def get_next_detections_id(self, camera_id: str) -> str:
        pass

class MainProcessor(BaseProcessor):
    # frame ingestion, detection, tracking, refpoints/speeds/zones, events + live state
    # stores: stream, detections, track state info, refpoints/speeds/zones, events
    # updates and serves: live state + annotated frame

    def __init__(self) -> None:
        self._frame_getter: FrameGetter = FrameGetter()
        self._detector: ObjectDetector = ObjectDetector()

    async def _run_for_camera(self, camera_id: str) -> None:
        while True:
            # get new frame
            frame_id: str = await self._frame_getter.get_next_frame_id(camera_id)
            # get detections
            detections_id: str = await self._detector.detect(frame_id)
            # get tracking info
            # get refpoints/speeds/zones
            # get (events) and live state info
            # update live state info for that camera
            pass

    @override
    async def start_camera(self, camera_id: str) -> None:
        pass

    @override
    async def stop_camera(self, camera_id: str) -> None:
        pass

    async def get_live_state_for_camera(self, camera_id: str) -> LiveState:
        pass