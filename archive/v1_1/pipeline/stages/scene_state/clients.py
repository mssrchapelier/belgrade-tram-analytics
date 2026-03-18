from abc import ABC, abstractmethod

from archive.v1_1.pipeline.stages.base.message import RealtimeRetrievalRequest
from archive.v1_1.pipeline.stages.vehicle_info import VehicleInfo


class VehicleInfoProto(ABC):

    @abstractmethod
    async def retrieve_for_realtime(self, request: RealtimeRetrievalRequest) -> VehicleInfo:
        pass

# implement ...