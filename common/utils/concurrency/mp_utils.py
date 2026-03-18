from multiprocessing import Process, Event
from multiprocessing.queues import Queue as QueueType
from multiprocessing.synchronize import Event as EventType
from queue import Empty as QueueEmptyException


def update_single_item_queue[T](item: T, q: QueueType[T]):
    """
    The queue is meant to contain one item only
    and have one producer (the calling worker) and one consumer.
    For this item, if the queue is not empty, empty it and then put the item in it.
    This ensures that the consumer will always get the newest item
    that is available by the time it calls `q.get()` and its execution begins.

    Important: SINGLE PRODUCER ONLY, SINGLE CONSUMER ONLY, single item,
               JUST A MOCKUP, NOT SCALABLE AT ALL
    TODO: Implement a pub-sub (perhaps) and ideally an in-memory ring buffer
    """
    try:
        while True:
            # remove all items from the queue until getting an Empty
            q.get_nowait()
    except QueueEmptyException:
        # the queue is already empty -- OK
        pass
    # put the new item
    q.put(item)

class ShutdownableProcess(Process):

    """
    A subclass of Process that defines a `multiprocessing.synchronize.Event` as an exit event.
    Subclasses should check for its status using `is_exit_signal()`
    and initiate shutdown logic when the returned value is `True`.
    The parent process can call the instance's `shutdown()` method to set the exit event
    and to trigger shutdown logic execution in this way.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._exit_event: EventType = Event()

    def is_exit_signal(self) -> bool:
        return self._exit_event.is_set()

    def shutdown(self):
        self._exit_event.set()

def stop_shutdownable(process: ShutdownableProcess,
                      *, timeout_per_join: float) -> None:
    name: str = process.name
    if process.is_alive():
        print(f"Shutting down process: {name}...")
        process.shutdown()
        process.join(timeout=timeout_per_join)
        if process.is_alive():
            print(f"Timed out waiting for the process to shut down, terminating: {name}...")
            process.terminate()
            process.join(timeout=timeout_per_join)
            if process.is_alive() and hasattr(process, "kill"):
                print(f"Timed out waiting for the process to terminate, killing: {name}...")
                process.kill()
                process.join(timeout=timeout_per_join)
    if not process.is_alive():
        process.close()
        print(f"Process shut down: {name}")
    else:
        raise RuntimeError(f"Failed to shut down the process: {name}")
