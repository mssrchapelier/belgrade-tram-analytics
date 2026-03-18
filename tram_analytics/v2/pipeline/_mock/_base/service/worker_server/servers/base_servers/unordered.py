from copy import copy
from typing import override

from tram_analytics.v2.pipeline._base.models.message import WorkerJobID
from tram_analytics.v2.pipeline._base.worker_servers.base_worker_server import BaseWorkerServerConfig
from tram_analytics.v2.pipeline._base.worker_servers.unordered_worker_server import BaseUnorderedWorkerServer
from tram_analytics.v2.pipeline._mock.common.dto.messages import FrameJobInProgressMessage, CriticalErrorMessage


# TODO: remove the "base" message DTOs (unneeded and create clutter in generics)

class UnorderedWorkerServer[
    InputT, OutputT, ConfigT: BaseWorkerServerConfig
](
    BaseUnorderedWorkerServer[
        FrameJobInProgressMessage, FrameJobInProgressMessage, CriticalErrorMessage,
        InputT, OutputT, ConfigT
    ]
):

    @override
    def _build_out_message(self, input_msg: FrameJobInProgressMessage) -> FrameJobInProgressMessage:
        return copy(input_msg)

    @override
    def _build_critical_error_message(self, job_id: WorkerJobID, exc: Exception) -> CriticalErrorMessage:
        return CriticalErrorMessage(details=f"{type(exc).__name__}")