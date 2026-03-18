from typing import Tuple, Iterator, List, AsyncGenerator
from datetime import datetime, timedelta
from io import BytesIO
from time import sleep
from pathlib import Path
import asyncio
from asyncio import TaskGroup
from asyncio.tasks import Task

import numpy as np
from numpy.typing import NDArray
from numpy.random import default_rng
from PIL import Image

from archive.v_a1.get_time import get_current_timestamp
from archive.v_a1.frame_producer import SynchronousFrameEmitter, AsyncFrameEmitter, FrameItemRaw

IMG_DIMENSIONS: Tuple[int, int] = (50, 50) # w, h
IMG_FORMAT: str = "png"
FRAME_EMITTER_TIMEOUT: float = 3.0 # in seconds

class MockSyncFrameEmitter(SynchronousFrameEmitter):

    def __init__(self, *, link: str, camera_id: str,
                 img_dimensions: Tuple[int, int],
                 img_format: str,
                 timeout: float):
        super().__init__(link, camera_id)
        self.img_dimensions: Tuple[int, int] = img_dimensions
        self.img_format: str = img_format
        self._timeout: float = timeout

    def __iter__(self) -> FrameItemRaw:
        # simulate yielding items with timestamps starting at the time
        # of the iterator's initialisation, and all subsequent items
        # being spaced exactly self._timeout seconds apart.
        # If an item is requested earlier than it would have been generated,
        # the thread is suspended until the scheduled generation time.
        next_generation_time: datetime = datetime.now()
        while True:
            current_time: datetime = datetime.now()
            if current_time < next_generation_time:
                sleep_duration: float = (next_generation_time - current_time).total_seconds()
                sleep(sleep_duration)
            yield self._generate_item()
            next_generation_time += timedelta(seconds=self._timeout)

    def _generate_item(self) -> FrameItemRaw:
        timestamp: str = get_current_timestamp()
        image: bytes = self._generate_image()
        return FrameItemRaw(camera_id=self._camera_id,
                            image=image,
                            timestamp=timestamp)

    def _generate_image(self) -> bytes:
        """
        Generates a PNG image filled with a random colour (and the dimensions of IMG_DIMENSIONS).
        Returns the image as a byte array.
        """
        rand_arr: NDArray[np.uint8] = default_rng().integers(low=0, high=255, size=3, dtype=np.uint8)
        img_arr: NDArray[np.uint8] = np.full(shape=(*self.img_dimensions, 3),
                                            fill_value=rand_arr,
                                            dtype=np.uint8)
        with Image.fromarray(img_arr) as img_pil, BytesIO() as img_stream: # type: Image, BytesIO
            img_pil.save(img_stream, format=self.img_format)
            img_bytes: bytes = img_stream.getvalue()
        return img_bytes

def _test_sync_emitter():
    num_to_generate: int = 10
    out_img_dir: Path = Path("REDACTED/test_sync_emitter")
    out_img_dir.mkdir(parents=True)

    emitter: MockSyncFrameEmitter = MockSyncFrameEmitter(
        link="", camera_id="cam_1",
        img_dimensions=IMG_DIMENSIONS, img_format=IMG_FORMAT,
        timeout=1.2
    )
    emitter_iter: Iterator[FrameItemRaw] = iter(emitter)
    for idx in range(num_to_generate): # type: int
        item: FrameItemRaw = next(emitter_iter)
        print("Camera {cam_id} generated image {img_idx} at time: {img_time}".format(
            cam_id=item.camera_id, img_idx=idx, img_time=item.timestamp
        ))
        with BytesIO(item.image) as img_bytes: # type: BytesIO
            with Image.open(img_bytes) as img_pil: # type: Image
                out_img_path: Path = out_img_dir.joinpath(f"{idx}.{img_pil.format}")
                img_pil.save(out_img_path)

async def _test_async_frame_emitter():

    async def _run_single_emitter(emitter: AsyncFrameEmitter,
                                  num_img: int):
        emitter_iter: AsyncGenerator[FrameItemRaw] = aiter(emitter)
        for idx in range(num_img): # type: int
            item: FrameItemRaw = await anext(emitter_iter)
            print("Camera {cam_id} generated image {img_idx} at time: {img_time}".format(
                cam_id=item.camera_id, img_idx=idx, img_time=item.timestamp
            ))

    # num of images to generate by each emitter
    num_to_generate: int = 10

    emitters: List[AsyncFrameEmitter] = [
        AsyncFrameEmitter(sync_emitter=MockSyncFrameEmitter(
            link="", camera_id=f"cam_{idx}",
            img_dimensions=IMG_DIMENSIONS, img_format=IMG_FORMAT,
            timeout=timeout
        ))
        for idx, timeout in zip([1, 2, 3], [0.7, 1.8, 3.1])
    ]
    # cam 1: 0.7 s, 5 images
    # cam 2: 1.2 s, 8 images
    # cam 3: 3.1 s, 3 images
    async with TaskGroup() as tg: # type: TaskGroup
        tasks: List[Task] = [
            tg.create_task(_run_single_emitter(emitter, num_img))
            for emitter, num_img in zip(emitters, [5, 8, 3])
        ]


if __name__ == "__main__":
    # _test_sync_emitter()
    asyncio.run(_test_async_frame_emitter())