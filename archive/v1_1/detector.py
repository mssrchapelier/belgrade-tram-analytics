from typing import override, List, Iterator, Dict, NamedTuple
import multiprocessing as mp
from multiprocessing import Process
from multiprocessing.queues import Queue as MpQueueType
from queue import Queue
from threading import Thread, Lock
import asyncio
from asyncio import Future
from asyncio import AbstractEventLoop
import itertools

class Frame(NamedTuple):
    frame_id: str

class Detection(NamedTuple):
    frame_id: str
    detection_id: str

class DetectorWorker(Process):

    """
    Wrapper for a single detection model running in its own process.
    Continuously consumes frames from a queue and pushes detections to another queue.
    """

    def __init__(self, *args,
                 frame_queue: MpQueueType[Frame], detections_queue: MpQueueType[Detection],
                 **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.frame_queue: MpQueueType[Frame] = frame_queue
        self._detections_queue: MpQueueType[Detection] = detections_queue

    def _detect(self, frame: Frame) -> Detection:
        # ... run inference ...
        raise NotImplementedError()

    @override
    def run(self) -> None:
        # ... load model here ...
        while True:
            frame: Frame = self.frame_queue.get()
            detections: Detection = self._detect(frame)
            self._detections_queue.put(detections)

class DetectionService:

    """
    A wrapper for a service running several detector workers in parallel
    (as separate processes) and distributing frames round-robin.
    Exposes a single async endpoint to get detections for a frame.
    Internally, maintains:
    (1) one queue (threading) for all incoming frames;
    (2) a frame queue (multiprocessing) for each detector;
    (3) a thread worker moving frames from 1) to 2) (load distributor);
    (4) a common multiprocessing queue to which detections from all detectors are published;
    (5) a buffer for futures that will return pending detections, keyed by frame id;
    (6) a thread worker getting detections from 4) and setting them
        as the result of the corresponding futures in 5).
    The main coroutine, having received a frame, puts it into 1), registers a future in (5) and awaits it.
    Once the detection is available in (4), the worker in (6) schedules the callback.
    The coroutine retrieves the result, deregisters the kvp from (5) and returns.

    TODO: manage timeouts (later)
    """

    def __init__(self, *, num_workers: int) -> None:
        # queue to retrieve detections from
        self._detections_queue: MpQueueType[Detection] = mp.Queue()
        # detector worker processes
        self._detection_workers: List[DetectorWorker] = [
            DetectorWorker(frame_queue=mp.Queue(), detections_queue=self._detections_queue)
            for idx in range(num_workers)
        ]

        # queue into which to put frames, and from which to forward them to detector workers
        self._frames_to_send_to_detector: Queue[Frame] = Queue()
        # frame id -> detections futures
        self._pending_detections: Dict[str, Future[Detection]] = dict()
        self._pending_detections_lock: Lock = Lock()

        # thread worker to distribute frames between detectors
        self._frame_enqueuer: Thread = Thread(target=self._send_frames_to_detector_workers,
                                              daemon=True)
        # thread worker to retrieve detections and set the completion of futures
        self._detection_dequeuer: Thread = Thread(target=self._dequeue_detections,
                                                  daemon=True)

        # the event loop handle to pass to thread workers
        self._event_loop: AbstractEventLoop = asyncio.get_running_loop()

    def _send_frames_to_detector_workers(self) -> None:
        """
        Send frames to detectors, in a round-robin fashion.
        """
        detector_iter: Iterator[DetectorWorker] = itertools.cycle(self._detection_workers)
        while True:
            # select the next worker
            worker: DetectorWorker = next(detector_iter)
            # retrieve the next frame
            frame: Frame = self._frames_to_send_to_detector.get()
            # push the frame to this worker
            worker.frame_queue.put(frame)

    def _dequeue_detections(self) -> None:
        """
        Get detections that are ready and schedule callbacks in the event loop
        to set the results of the associated futures to these detections,
        for the awaiting coroutines to collect and return.
        """
        while True:
            # retrieve detection
            det: Detection = self._detections_queue.get()
            frame_id: str = det.frame_id
            with self._pending_detections_lock:
                # retrieve the future for the corresponding frame id, if it exists
                future: Future[Detection] | None = self._pending_detections.get(frame_id, None)
            if future is None:
                # drop the detection (frame id may have been deregistered because of a timeout)
                # TODO: log
                pass
            else:
                # schedule setting the result of this future to the retrieved detection
                self._event_loop.call_soon_threadsafe(future.set_result, det)

    def start(self) -> None:
        self._detection_dequeuer.start()
        for worker in self._detection_workers: # type: DetectorWorker
            worker.start()
        self._frame_enqueuer.start()

    def stop(self) -> None:
        # currently, will never return! Workers need to subclass ShutdownableProcess
        # (already implemented for that purpose elsewhere)
        self._frame_enqueuer.join()
        for worker in self._detection_workers: # type: DetectorWorker
            worker.join()
        self._detection_dequeuer.join()

    def _register_future_for_frame(self, frame_id: str) -> Future[Detection]:
        # initialise the future
        future: Future[Detection] = self._event_loop.create_future()
        with self._pending_detections_lock:
            if frame_id in self._pending_detections:
                raise RuntimeError(f"Can't register a future for detection for frame {frame_id}: already registered")
            # register under the frame id
            self._pending_detections[frame_id] = future
        return future

    async def detect(self, frame: Frame) -> Detection:
        frame_id: str = frame.frame_id

        # register a future (to be fulfilled when the detection becomes available)
        future: Future[Detection] = self._register_future_for_frame(frame_id)
        # send the frame to the queue
        self._frames_to_send_to_detector.put(frame)

        try:
            # await the future and retrieve the detection
            detection: Detection = await future
            return detection
        finally:
            # deregister the future (if the frame id key is registered)
            with self._pending_detections_lock:
                self._pending_detections.pop(frame_id, None)