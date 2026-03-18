import logging
from abc import ABC, abstractmethod
from logging import Logger
from warnings import deprecated

from common.utils.logging_utils.logging_utils import get_logger_name_for_object
from tram_analytics.v2.pipeline._base.models.message import BaseFrameJobInProgressMessage, BaseCriticalErrorMessage, \
    MessageWithAckFuture
from tram_analytics.v2.pipeline._base.worker_servers.base_worker_server import BaseWorkerServerConfig, BaseWorkerServer

@deprecated(
    "Deprecated; inject BaseWorkerServer instance into the MQ broker-specific adapter instead"
)
class BaseWorkerServerInputAdapter_Old[
    InputMsgT: BaseFrameJobInProgressMessage,
    OutputMsgT: BaseFrameJobInProgressMessage,
    CriticalErrMsgT: BaseCriticalErrorMessage,
    InputT, OutputT,
    ConfigT: BaseWorkerServerConfig
](ABC):

    def __init__(self,
                 worker_server: BaseWorkerServer[
                     InputMsgT, OutputMsgT, CriticalErrMsgT, InputT, OutputT, ConfigT
                 ]) -> None:
        self._worker_server: BaseWorkerServer[
            InputMsgT, OutputMsgT, CriticalErrMsgT, InputT, OutputT, ConfigT
        ] = worker_server
        self._logger: Logger = logging.getLogger(get_logger_name_for_object(self))

    async def start(self) -> None:
        await self._worker_server.start()
        await self._after_worker_server_startup()

    @abstractmethod
    async def _after_worker_server_startup(self) -> None:
        pass

    async def shutdown(self) -> None:
        await self._before_worker_server_shutdown()
        await self._worker_server.shutdown()

    @abstractmethod
    async def _before_worker_server_shutdown(self) -> None:
        pass

    def on_receive(self, input_msg_with_ack_future: MessageWithAckFuture[InputMsgT]) -> None:
        self._worker_server.on_receive(input_msg_with_ack_future)
