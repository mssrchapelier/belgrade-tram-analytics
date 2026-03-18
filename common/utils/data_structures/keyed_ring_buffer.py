from typing import Dict, Hashable, Deque
from collections import deque

# ring buffer with fast random access

class KeyNotInBuffer(Exception):
    pass

class KeyedRingBuffer[KeyT: Hashable, ValueT]:

    def __init__(self, max_size: int):
        self._validate_maxsize(max_size)

        self._max_size: int = max_size
        # key -> value
        self._map: Dict[KeyT, ValueT] = dict()
        # a deque of keys, used to track which keys are too old and must be evicted
        self._keys: Deque[KeyT] = deque(maxlen=self._max_size)

    @staticmethod
    def _validate_maxsize(max_size: int) -> None:
        if max_size <= 0:
            raise ValueError(f"max_size must be a positive integer, got: {max_size}")

    def upsert(self, *, key: KeyT, value: ValueT) -> None:
        if len(self._map) > self._max_size:
            raise RuntimeError("Invalid state: the number of items stored in the buffer exceeds its set capacity "
                               f"(stored {len(self._map)}, capacity {self._max_size})")
        elif len(self._map) == self._max_size:
            # discard the oldest key
            oldest_key: KeyT = self._keys.popleft()
            # discard the value for it
            self._map.pop(oldest_key)
        # NOTE: BEHAVIOUR:
        # Allows updating an already existing key
        self._map[key] = value
        self._keys.append(key)

    def get(self, key: KeyT) -> ValueT:
        if key not in self._map:
            # custom exception for easier catching in the calling code if necessary
            raise KeyNotInBuffer(f"Key {key} not found in the buffer")
        return self._map[key]

    def clear(self) -> None:
        self._map.clear()
        self._keys.clear()