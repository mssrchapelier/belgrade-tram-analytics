from collections import OrderedDict
from threading import Lock
from typing import OrderedDict as OrderedDictType

from tram_analytics.v1.models.pipeline_artefacts import PipelineArtefacts
from tram_analytics.v1.pipeline.server.helpers.packet import PipelinePacket


class StaleReferenceException(Exception):
    pass

class PipelineCache:
    """
    An implementation for a cache for instances of `PipelinePacket`
    that stores at most `max_len` items, keyed by frame ID.
    Exposes `push()`, which removes the key-value pair for the oldest added item
    if the cache is full, then adds a key-value pair for the new item.
    """

    def __init__(self, max_len: int):
        if not (isinstance(max_len, int) and max_len > 0):
            raise ValueError(f"max_len must be a positive integer, got: {max_len}")

        # frame_id -> PipelinePacket
        self._cache: OrderedDictType[str, PipelinePacket] = OrderedDict()
        self._latest_key: str | None = None
        self._max_len: int = max_len
        # a threading lock to use whenever updating the cache
        self._cache_lock: Lock = Lock()

    def push(self, item: PipelinePacket) -> None:
        """
        If the cache has reached the max length, discard the oldest key-value pair.
        Put the item into the cache, with `item.artefacts.frame_metadata.frame_id` used as the key.
        """
        frame_id: str = item.artefacts.frame_metadata.frame_id
        with self._cache_lock:
            is_full: bool = len(self._cache) >= self._max_len
            if is_full:
                # discard the oldest key-value pair
                self._cache.popitem(last=False)
            # put the new item
            self._cache[frame_id] = item
            # update the latest key
            self._latest_key = frame_id

    def _get_latest(self) -> PipelinePacket:
        if self._latest_key is None:
            raise RuntimeError("Tried to access the latest item in the cache "
                               "whose _latest_key is set to None (no data yet?)")
        packet: PipelinePacket | None = self._cache.get(self._latest_key)
        if packet is None:
            raise RuntimeError(f"The value stored as the cache's latest key ({self._latest_key}) "
                               f"was not found in the cache")
        return packet

    def get_latest_artefacts(self) -> PipelineArtefacts:
        latest: PipelinePacket = self._get_latest()
        return latest.artefacts

    def get_image_by_id(self, frame_id: str) -> bytes:
        packet: PipelinePacket | None = self._cache.get(frame_id)
        if packet is None:
            raise StaleReferenceException(f"Frame packet {frame_id} not found in the cache (already discarded?)")
        return packet.annotated_image
