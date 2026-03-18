from typing import Callable, Dict, Iterable, Set, Iterator, Tuple


class AliveObjectHistoryManager[Stored]:

    def __init__(self, factory: Callable[[], Stored]) -> None:
        self._history: Dict[str, Stored] = dict()
        self._factory: Callable[[], Stored] = factory

    def update_ids(self, alive_ids: Iterable[str]) -> None:
        alive_ids_set: Set[str] = set(alive_ids)
        prev_alive_ids: Set[str] = set(self._history.keys())
        new_ids: Set[str] = alive_ids_set.difference(prev_alive_ids)
        dead_ids: Set[str] = prev_alive_ids.difference(alive_ids_set)

        # remove the mappings for dead ids
        for dead_id in dead_ids: # type: str
            self._history.pop(dead_id)
        # add mappings for new ids
        for new_id in new_ids: # type: str
            self._history[new_id] = self._factory()

    def __getitem__(self, obj_id: str) -> Stored:
        try:
            return self._history[obj_id]
        except KeyError as e:
            msg: str = (f"ID {obj_id} not in history (call this instance's update_ids() first "
                        f"to add new IDs and remove dead IDs)")
            raise KeyError(msg) from e

    def __iter__(self) -> Iterator[Tuple[str, Stored]]:
        for key, value in self._history.items(): # type: str, Stored
            yield key, value

    def __len__(self) -> int:
        return len(self._history)
