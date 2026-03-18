from typing import NamedTuple, AsyncIterator, List, override
from time import perf_counter
import asyncio
from asyncio import Task, TaskGroup

class Detection(NamedTuple):
    frame_id: int

class Frame(NamedTuple):
    frame_id: int

class FrameGetter:

    def __init__(self) -> None:
        self._next_idx: int = 0

    async def __aiter__(self) -> AsyncIterator[Frame]:
        while True:
            await asyncio.sleep(0.8)
            frame: Frame = Frame(frame_id=self._next_idx)
            self._next_idx += 1
            yield frame

    async def get_next_frame(self) -> Frame:
        await asyncio.sleep(0.8)
        frame: Frame = Frame(frame_id=self._next_idx)
        self._next_idx += 1
        return frame

class Detector:

    async def detect(self, frame: Frame) -> Detection:
        await asyncio.sleep(2.0)
        return Detection(frame_id=frame.frame_id)

class Processor:

    def __init__(self) -> None:
        self._frame_getter: FrameGetter = FrameGetter()
        self._detector: Detector = Detector()

    async def get_next_detection(self) -> Detection:
        frame: Frame = await self._frame_getter.get_next_frame()
        print(f"Got frame {frame.frame_id}")
        det: Detection = await self._detector.detect(frame)
        print(f"Got detection {det.frame_id}")
        return det

async def sandbox():
    total: int = 5
    processor: Processor = Processor()
    start_time: float = perf_counter()
    for idx in range(total):
        det: Detection = await processor.get_next_detection()
    end_time: float = perf_counter()
    elapsed: float = end_time - start_time
    print(f"Completed in {elapsed:.1f} s")

async def sandbox_2():
    total: int = 5
    processor: Processor = Processor()
    tasks: List[Task[Detection]] = []
    start_time: float = perf_counter()
    async with TaskGroup() as tg: # type: TaskGroup
        for idx in range(total):
            tasks.append(tg.create_task(processor.get_next_detection()))
    end_time: float = perf_counter()
    elapsed: float = end_time - start_time
    print(f"Completed in {elapsed:.1f} s")

if __name__ == "__main__":
    asyncio.run(
        # sandbox()
        sandbox_2()
    )