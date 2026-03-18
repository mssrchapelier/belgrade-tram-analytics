from typing import Dict, List, Set

from pydantic import BaseModel

from archive.v1.src.models.models import (
    Track, TrackState_Old as PipelineTrackState, TrackerStepOutputBuilder
)
from tram_analytics.v1.models.common_types import BoundingBox
from tram_analytics.v1.models.components.tracking import DetectionToTrackState


class TrackState(BaseModel):
    is_matched: bool
    is_confirmed: bool
    bbox: BoundingBox


class TrackWithHistory(BaseModel):
    track_id: str
    class_id: int
    history: List[TrackState]

class TrackWithHistoryBuilder:

    def __init__(self, track_id: str, class_id: int):
        self.track_id: str = track_id
        self.class_id: int = class_id
        self.history: List[TrackState] = []

    def export(self) -> TrackWithHistory:
        return TrackWithHistory(
            track_id=self.track_id,
            class_id=self.class_id,
            # Not a deep copy, but only references to instances of DrawingTrackState are needed.
            # A shallow copy is still needed because this object's `.history` field
            # is being mutated in `TrackDataRecorder`.
            history=self.history.copy()
        )


class VisualizerInputBuilder:

    def __init__(self):
        # track_id -> TrackWithHistoryBuilder
        self.out_tracks: Dict[str, TrackWithHistoryBuilder] = dict()

    def _add_new_tracks(self, tracks: List[Track]):
        for track in tracks: # type: Track
            track_id: str = track.track_id
            assert track_id not in self.out_tracks
            new_track_with_history: TrackWithHistoryBuilder = TrackWithHistoryBuilder(track_id, track.class_id)
            self.out_tracks[track_id] = new_track_with_history

    @staticmethod
    def _get_matched_track_ids(states: List[PipelineTrackState],
                               state_to_det_mappings: List[DetectionToTrackState]) -> Set[str]:
        # if a state ID is in the mappings, it means it matched some detection
        matched_state_ids: Set[str] = {
            mapping.track_state_id
            for mapping in state_to_det_mappings
        }
        # matched_track_ids: Set[str] = {
        #     state.track_id
        #     for state in states # type: PipelineTrackState
        #     if state.track_state_id in matched_state_ids
        # }
        return matched_state_ids


    @staticmethod
    def _build_track_state(input_state: PipelineTrackState,
                           *, is_matched: bool):
        is_confirmed: bool = input_state.is_confirmed_track
        drawing_track_state: TrackState = TrackState(
            is_matched=is_matched, is_confirmed=is_confirmed,
            bbox=input_state.bbox.model_copy()
        )
        return drawing_track_state

    def _update_histories(self, pipeline_output: TrackerStepOutputBuilder):
        input_states: List[PipelineTrackState] = pipeline_output.track_states

        matched_state_ids: Set[str] = self._get_matched_track_ids(
            input_states, pipeline_output.track_state_to_detection_mappings
        )

        for state in input_states:  # type: PipelineTrackState
            track_id: str = state.track_id
            assert track_id in self.out_tracks
            is_matched: bool = state.track_state_id in matched_state_ids
            drawing_state: TrackState = self._build_track_state(
                state, is_matched=is_matched
            )
            self.out_tracks[track_id].history.append(drawing_state)


    def _remove_dead_tracks(self, input_states: List[PipelineTrackState]):
        # tracks whose track IDs are not present in input_states are no longer alive;
        # remove them from the map
        alive_track_ids: Set[str] = {state.track_id for state in input_states}
        dead_track_ids: Set[str] = set(self.out_tracks.keys()).difference(alive_track_ids)
        for track_id in dead_track_ids:  # type: str
            self.out_tracks.pop(track_id)

    def _export_all_tracks(self) -> List[TrackWithHistory]:
        return [
            track.export()
            for track in self.out_tracks.values()
        ]

    def update(self, pipeline_output: TrackerStepOutputBuilder) -> List[TrackWithHistory]:
        input_states: List[PipelineTrackState] = pipeline_output.track_states
        # check that there are no duplicate track ids among the track states
        assert len(set(state.track_id for state in input_states)) == len(input_states)

        self._add_new_tracks(pipeline_output.new_tracks)
        self._update_histories(pipeline_output)
        self._remove_dead_tracks(input_states)

        exported: List[TrackWithHistory] = self._export_all_tracks()
        return exported
