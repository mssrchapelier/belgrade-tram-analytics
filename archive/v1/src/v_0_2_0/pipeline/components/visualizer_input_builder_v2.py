__version__ = "0.2.0"

from typing import Literal, TypeAlias, Annotated, List, Dict, Set, Type

from pydantic import BaseModel, Field

from archive.v1.src.v_0_2_0.pipeline.components.analytics.trams import TramPositionalProxies
from archive.v1.src.models.models import TrackState_Old as PipelineTrackState, Track
from tram_analytics.v1.models.common_types import BoundingBox
from tram_analytics.v1.models.components.tracking import DetectionToTrackState
from archive.v1.src.models.models import TrackerStepOutput_Old
from archive.v1.src.v_0_2_0.pipeline.components.analytics.analytics_postprocessor_old import AnalyticsPostprocessorOutput, TramStateInfo, VehicleType, \
    CLASS_ID_TO_VEHICLE_TYPE


class BaseEnhancedTrackState(BaseModel):
    is_matched: bool
    is_confirmed: bool
    bbox: BoundingBox


class CarEnhancedTrackState(BaseEnhancedTrackState):
    vehicle_type: Literal[VehicleType.CAR] = VehicleType.CAR


class TramEnhancedTrackState(BaseEnhancedTrackState):
    vehicle_type: Literal[VehicleType.TRAM] = VehicleType.TRAM
    corridor_id: int | None
    proxies: TramPositionalProxies | None


EnhancedTrackState: TypeAlias = Annotated[
    CarEnhancedTrackState | TramEnhancedTrackState,
    Field(discriminator="vehicle_type")
]


class BaseEnhancedTrackWithHistory(BaseModel):
    track_id: str
    class_id: int


class CarEnhancedTrackWithHistory(BaseEnhancedTrackWithHistory):
    vehicle_type: Literal[VehicleType.CAR] = VehicleType.CAR
    history: List[CarEnhancedTrackState]


class TramEnhancedTrackWithHistory(BaseEnhancedTrackWithHistory):
    vehicle_type: Literal[VehicleType.TRAM] = VehicleType.TRAM
    history: List[TramEnhancedTrackState]


EnhancedTrackWithHistory: TypeAlias = Annotated[
    CarEnhancedTrackWithHistory | TramEnhancedTrackWithHistory,
    Field(discriminator="vehicle_type")
]

class EnhancedTrackWithHistoryBuilder:

    def __init__(self, track_id: str, class_id: int):
        self.track_id: str = track_id
        self.class_id: int = class_id

        vehicle_type: VehicleType | None = CLASS_ID_TO_VEHICLE_TYPE.get(class_id)
        if vehicle_type is None:
            raise ValueError(f"Class ID {class_id} not in CLASS_ID_TO_VEHICLE_TYPE")
        self.vehicle_type: VehicleType = vehicle_type

        self.history: List[EnhancedTrackState] = []

    def export(self) -> EnhancedTrackWithHistory:
        track_class: Type[EnhancedTrackWithHistory] = (
            CarEnhancedTrackWithHistory if self.vehicle_type == VehicleType.CAR
            else TramEnhancedTrackWithHistory
        )
        return track_class(
            track_id=self.track_id,
            class_id=self.class_id,
            # Not a deep copy, but only references to instances of DrawingTrackState are needed.
            # A shallow copy is still needed because this object's `.history` field
            # is being mutated in `TrackDataRecorder`.
            history=self.history.copy()
        )

class VisualizerInputBuilderV2:

    def __init__(self):
        # track_id -> EnhancedTrackWithHistoryBuilder
        self.out_tracks: Dict[str, EnhancedTrackWithHistoryBuilder] = dict()

    def _add_new_tracks(self, tracks: List[Track]):
        for track in tracks: # type: Track
            track_id: str = track.track_id
            assert track_id not in self.out_tracks
            new_track_with_history: EnhancedTrackWithHistoryBuilder = EnhancedTrackWithHistoryBuilder(track_id, track.class_id)
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
    def _build_car_track_state(input_state: PipelineTrackState,
                               *,
                               is_matched: bool) -> CarEnhancedTrackState:
        is_confirmed: bool = input_state.is_confirmed_track
        output: CarEnhancedTrackState = CarEnhancedTrackState(
            is_matched=is_matched, is_confirmed=is_confirmed,
            bbox=input_state.bbox.model_copy()
        )
        return output

    @staticmethod
    def _build_tram_track_state(input_state: PipelineTrackState,
                                tram_state_info: TramStateInfo,
                                *,
                                is_matched: bool) -> TramEnhancedTrackState:
        is_confirmed: bool = input_state.is_confirmed_track
        output: TramEnhancedTrackState = TramEnhancedTrackState(
            is_matched=is_matched, is_confirmed=is_confirmed,
            bbox=input_state.bbox,
            corridor_id=tram_state_info.corridor_id,
            proxies=tram_state_info.proxies
        )
        return output

    def _update_histories(self, pipeline_output: TrackerStepOutput_Old,
                          tram_states_info: List[TramStateInfo]):
        input_states: List[PipelineTrackState] = pipeline_output.track_states

        # track_state_id -> TramStateInfo
        state_id_to_tram_info: Dict[str, TramStateInfo] = {
            item.track_state_id: item
            for item in tram_states_info
        }

        matched_state_ids: Set[str] = self._get_matched_track_ids(
            input_states, pipeline_output.track_state_to_detection_mappings
        )

        for state in input_states:  # type: PipelineTrackState
            is_tram_state: bool = state.track_state_id in state_id_to_tram_info

            track_id: str = state.track_id
            assert track_id in self.out_tracks
            state_id: str = state.track_state_id
            is_matched: bool = state.track_state_id in matched_state_ids
            drawing_state: EnhancedTrackState = (
                self._build_tram_track_state(state, state_id_to_tram_info[state_id],
                                             is_matched=is_matched)
                if is_tram_state
                else self._build_car_track_state(state, is_matched=is_matched)
            )
            self.out_tracks[track_id].history.append(drawing_state)


    def _remove_dead_tracks(self, input_states: List[PipelineTrackState]):
        # tracks whose track IDs are not present in input_states are no longer alive;
        # remove them from the map
        alive_track_ids: Set[str] = {state.track_id for state in input_states}
        dead_track_ids: Set[str] = set(self.out_tracks.keys()).difference(alive_track_ids)
        for track_id in dead_track_ids:  # type: str
            self.out_tracks.pop(track_id)

    def _export_all_tracks(self) -> List[EnhancedTrackWithHistory]:
        return [
            track.export()
            for track in self.out_tracks.values()
        ]

    def update(self, pipeline_output: TrackerStepOutput_Old,
               analytics_output: AnalyticsPostprocessorOutput) -> List[EnhancedTrackWithHistory]:
        input_states: List[PipelineTrackState] = pipeline_output.track_states
        # check that there are no duplicate track ids among the track states
        assert len(set(state.track_id for state in input_states)) == len(input_states)

        self._add_new_tracks(pipeline_output.new_tracks)
        self._update_histories(pipeline_output, analytics_output.tram_states_info)
        self._remove_dead_tracks(input_states)

        exported: List[EnhancedTrackWithHistory] = self._export_all_tracks()
        return exported