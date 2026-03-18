from abc import ABC, abstractmethod
from typing import Dict


class BaseDictStore[StoredT](ABC):

    def __init__(self) -> None:
        self._data: Dict[str, StoredT] = dict()

    async def store(self, item: StoredT) -> None:
        frame_id: str = self._extract_frame_id_from_item(item)
        self._data[frame_id] = item

    async def retrieve(self, frame_id: str) -> StoredT:
        return self._data[frame_id]

    @classmethod
    @abstractmethod
    def _extract_frame_id_from_item(cls, item: StoredT) -> str:
        pass