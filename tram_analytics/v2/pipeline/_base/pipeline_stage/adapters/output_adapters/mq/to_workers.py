from abc import ABC, abstractmethod

from tram_analytics.v2.pipeline._base.models.message import WorkerOutputMessageWrapper, WorkerInputMessageWrapper


class StageMQToWorkersOutputPort[InputMsgT, OutputMsgT](ABC):

    """
    An abstract message queue broker adapter for the `BaseProcessor`
    to communicate with the worker (workers), either for the same specific camera
    (tracking, vehicle info, scene state) or camera-agnostic (detection, feature extraction).
    For the latter, for multiple workers, implement routing logic in subclasses.
    """

    @abstractmethod
    async def send_to_worker(self, msg: WorkerInputMessageWrapper[InputMsgT]) -> None:
        # serialise if needed per the protocol used
        pass

    @abstractmethod
    async def get_next_message_from_worker(self) -> WorkerOutputMessageWrapper[OutputMsgT]:
        # if any deserialisation is used at all, the subclasses
        # should define the logic to create and set exceptions to the future in the output item
        # where necessary

        # set up camera ID as the routing key, etc.
        # or just inject asyncio queues if running in the same process
        pass
