from typing import AsyncIterator, List, Tuple
from pathlib import Path
from fractions import Fraction
from datetime import datetime, timedelta
from io import BytesIO
import asyncio
from asyncio import Queue, TaskGroup, Task
import logging

import av
from av import VideoStream
from av.container import InputContainer
from av.video.frame import VideoFrame
from PIL import Image
from PIL.Image import Image as ImageType
from pydantic import BaseModel

DATETIME_FORMAT: str = "%Y%m%d_%H%M%S.%f"

QUEUE_MAX_SIZE: int = 15
BATCH_SIZE: int = 5

class FrameItem(BaseModel):
    image: bytes
    timestamp: str
    encoding: str
    idx: int

def build_frame_item(frame: VideoFrame,
                     *, frame_idx: int,
                     start_time: datetime,
                     timebase: Fraction,
                     target_dims: Tuple[int, int],  # w, h
                     ) -> FrameItem:
    pts: int | None = frame.pts
    assert pts is not None
    pts_sec: float = float(pts * timebase)
    frame_timestamp: datetime = start_time + timedelta(seconds=pts_sec)
    frame_timestamp_str: str = frame_timestamp.strftime(DATETIME_FORMAT)
    downsampled: VideoFrame = frame.reformat(width=target_dims[0],
                                             height=target_dims[1])
    with downsampled.to_image() as pil_img, BytesIO() as bytes_out:  # type: ImageType, BytesIO
        pil_img.save(fp=bytes_out, format="PNG")
        img_bytes: bytes = bytes_out.getvalue()
        item: FrameItem = FrameItem(image=img_bytes,
                                    timestamp=frame_timestamp_str,
                                    encoding="png",
                                    idx=frame_idx)
    return item


async def keyframe_getter(stream_url: str,
                          *, target_h: int) -> AsyncIterator[FrameItem]:
    """
    Reads keyframes from stream_url and yields FrameItem instances
    containing the frames and the PNG image bytes data.
    The image is resized to have the height of target_h,
    preserving the original aspect ratio.
    """
    container: InputContainer = av.open(stream_url)
    stream: VideoStream = container.streams.video[0]
    stream.codec_context.skip_frame = "NONKEY"

    start_time: datetime = datetime.now()
    print("Start time: {}".format(start_time.strftime(DATETIME_FORMAT)))

    timebase: Fraction | None = stream.time_base
    assert timebase is not None
    aspect_ratio: Fraction = Fraction(stream.codec_context.width, stream.codec_context.height)
    target_w: int = round(target_h * aspect_ratio)

    for frame_idx, frame in enumerate(container.decode(stream), start=1): # type: int, VideoFrame
        obj_to_yield: FrameItem = await asyncio.to_thread(
            build_frame_item,
            frame,
            frame_idx=frame_idx, start_time=start_time,
            timebase=timebase, target_dims=(target_w, target_h))
        logging.info(f"keyframe_getter: yielding {obj_to_yield.idx}")
        yield obj_to_yield

async def frame_producer(stream_url: str, queue: Queue[FrameItem | None],
                         *, target_h: int) -> None:
    try:
        async for frame_item in keyframe_getter(stream_url, target_h=target_h): # type: FrameItem
            logging.info(f"frame_producer: enqueueing {frame_item.idx}")
            await queue.put(frame_item)
    finally:
        # put a sentinel
        await queue.put(None)

def process_batch_sync(frames: List[FrameItem], *, out_parent_dir: Path, batch_idx: int) -> None:
    out_dir: Path = out_parent_dir.joinpath(f"{batch_idx:03d}")
    out_dir.mkdir(parents=True)
    for frame in frames: # type: FrameItem
        out_path: Path = out_dir.joinpath(f"{frame.timestamp}.{frame.encoding}")
        with BytesIO(frame.image) as img_stream: # type: BytesIO
            with Image.open(fp=img_stream) as pil_img: # type: ImageType
                pil_img.save(out_path, format=frame.encoding)
                logging.info(f"process_batch: saved {frame.idx}")
    logging.info(f"process_batch: batch processed")

async def process_batch(frames: List[FrameItem], *, out_parent_dir: Path, batch_idx: int) -> None:
    await asyncio.to_thread(process_batch_sync,
                            frames, out_parent_dir=out_parent_dir, batch_idx=batch_idx)

async def frame_consumer(queue: Queue[FrameItem | None], *, batch_size: int, out_dir: Path) -> None:
    batch: List[FrameItem] = []

    next_batch_idx: int = 1

    while True:
        item: FrameItem | None = await queue.get()
        # queue.task_done()
        if item is None:
            if batch:
                await process_batch(batch, out_parent_dir=out_dir, batch_idx=next_batch_idx)
            break
        batch.append(item)
        logging.info(f"frame_consumer: consumed {item.idx}, batch len {len(batch)}")
        if len(batch) >= batch_size:
            logging.info("frame_consumer: sending batch to processing")
            await process_batch(batch, out_parent_dir=out_dir, batch_idx=next_batch_idx)
            batch.clear()
            next_batch_idx += 1

async def extract_frames(*, stream_url: str, out_dir: Path, target_h: int):
    queue: Queue[FrameItem | None] = Queue(maxsize=QUEUE_MAX_SIZE)
    async with TaskGroup() as task_group: # type: TaskGroup
        producer_task: Task[None] = task_group.create_task(
            frame_producer(stream_url, queue, target_h=target_h)
        )
        consumer_task: Task[None] = task_group.create_task(
            frame_consumer(queue, batch_size=BATCH_SIZE, out_dir=out_dir)
        )
