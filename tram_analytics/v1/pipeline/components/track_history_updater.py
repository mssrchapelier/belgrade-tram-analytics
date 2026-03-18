from dataclasses import dataclass, field
from typing import Dict, List, Set

from tram_analytics.v1.models.components.tracking import TrackState, TrackHistory


@dataclass
class MutableTrackHistory:
    class_id: int
    history: List[TrackState] = field(default_factory=list)

class TrackHistoryUpdater:
    """
    Maintains a history of tracks that are alive,
    storing and updating track states for each of them
    and deleting tracks that are no longer alive.
    """

    def __init__(self):
        # track id -> Track
        self._tracks: Dict[str, MutableTrackHistory] = dict()

    def update(self, track_states_in_frame: List[TrackState]) -> None:
        prev_alive_track_ids: Set[str] = set(self._tracks.keys())
        cur_alive_track_ids: Set[str] = set()

        for state in track_states_in_frame: # type: TrackState
            track_id: str = state.track_id
            cur_alive_track_ids.add(track_id)
            class_id: int = state.class_id
            if track_id in self._tracks:
                # check that the class ID corresponds to the one stored for the track
                track: MutableTrackHistory = self._tracks[track_id]
                if class_id != track.class_id:
                    msg: str = (f"Got class_id {class_id}, expected {track.class_id} "
                                f"for track {track_id} (state {state.track_state_id})")
                    raise ValueError(msg)
            else:
                # create a new track
                track: MutableTrackHistory = MutableTrackHistory(class_id=class_id)
                self._tracks[track_id] = track
            # add this state to the track
            track.history.append(state)

        # remove track ids that are no longer alive (i. e. that did not appear in `track_states_for_frame`)
        dead_track_ids: Set[str] = set.difference(prev_alive_track_ids, cur_alive_track_ids)
        for t_id in dead_track_ids: # type: str
            self._tracks.pop(t_id)


    def export(self) -> List[TrackHistory]:
        """
        Export the current state.
        """
        out_obj: List[TrackHistory] = [
            TrackHistory(track_id=track_id,
                         class_id=mutable_history.class_id,
                         history=mutable_history.history.copy())
            for track_id, mutable_history in self._tracks.items()
        ]
        return out_obj