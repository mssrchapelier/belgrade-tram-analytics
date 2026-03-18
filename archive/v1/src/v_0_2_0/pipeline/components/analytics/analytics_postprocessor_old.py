from typing import List, Dict

from pydantic import BaseModel

from tram_analytics.v1.models.common_types import VehicleType
from tram_analytics.v1.models.components.detection import Detection
from tram_analytics.v1.models.components.frame_ingestion import Frame
from archive.v1.src.models.models import TrackState_Old, Track
from archive.v1.src.v_0_2_0.pipeline.components.analytics.trams import TramProcessor, TramProcessorOutput, TramPositionalProxies
from archive.v1.src.v_0_2_0.pipeline.components.analytics.scene_geometry.scene_geometry import SceneGeometryConfig
from archive.v1.src.models.models import TrackerStepOutput_Old

CLASS_ID_TO_VEHICLE_TYPE: Dict[int, VehicleType] = {
    0: VehicleType.TRAM,
    2: VehicleType.CAR
}

class AnalyticsPostprocessorInput(BaseModel):
    frame: Frame
    detections: List[Detection]
    tracking_results: TrackerStepOutput_Old
    # all tracks that are alive
    tracks: List[Track]

class TramStateInfo(BaseModel):
    track_state_id: str
    corridor_id: int | None
    proxies: TramPositionalProxies | None

class AnalyticsPostprocessorOutput(BaseModel):
    tram_states_info: List[TramStateInfo]

class AnalyticsPostprocessor:

    def __init__(self, scene_geometry_config: SceneGeometryConfig):
        self._tram_processor: TramProcessor = TramProcessor(scene_geometry_config.rail_corridors)

    def process_frame_outputs(self, inputs: AnalyticsPostprocessorInput) -> AnalyticsPostprocessorOutput:
        tram_states_info: List[TramStateInfo] = []

        # track ID -> Track
        tracks_by_id: Dict[str, Track] = {track.track_id: track for track in inputs.tracks}

        for track_state in inputs.tracking_results.track_states: # type: TrackState_Old
            track: Track = tracks_by_id[track_state.track_id]
            class_id: int = track.class_id
            if CLASS_ID_TO_VEHICLE_TYPE[class_id] == VehicleType.TRAM:
                tram_processor_output: TramProcessorOutput = self._tram_processor.process_tram_bbox(
                    track_state.bbox
                )
                state_info: TramStateInfo = TramStateInfo(track_state_id=track_state.track_state_id,
                                                          corridor_id=tram_processor_output.corridor_id,
                                                          proxies=tram_processor_output.proxies)
                tram_states_info.append(state_info)
        outputs: AnalyticsPostprocessorOutput = AnalyticsPostprocessorOutput(
            tram_states_info=tram_states_info
        )
        return outputs
