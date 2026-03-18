from datetime import datetime
from typing import override, Self

from pydantic import BaseModel, model_validator

from tram_analytics.v1.models.components.scene_state.events.base import BaseSceneEventsContainer
from tram_analytics.v1.models.components.scene_state.events.subprocessors.lifetime import \
    VehiclesLifetimeEventsContainer
from tram_analytics.v1.models.components.scene_state.events.subprocessors.motion_status.motion_status import \
    MotionStatusEventsContainer
from tram_analytics.v1.models.components.scene_state.events.subprocessors.speed import SpeedUpdatesContainer
from tram_analytics.v1.models.components.scene_state.events.subprocessors.zone_occupancy import \
    ZoneOccupancyEventsContainer
from tram_analytics.v1.models.components.scene_state.events.subprocessors.zone_transit import \
    ZoneTransitEventsContainer


class SceneEventsWrapper(BaseSceneEventsContainer):

    """
    A container for all events emitted for one frame.
    """

    vehicle_lifetime: VehiclesLifetimeEventsContainer
    speeds: SpeedUpdatesContainer
    zone_transit: ZoneTransitEventsContainer
    zone_occupancy: ZoneOccupancyEventsContainer
    motion_status: MotionStatusEventsContainer

    @override
    @classmethod
    def create_empty_container(cls) -> Self:
        return cls(
            vehicle_lifetime=VehiclesLifetimeEventsContainer.create_empty_container(),
            speeds=SpeedUpdatesContainer.create_empty_container(),
            zone_transit=ZoneTransitEventsContainer.create_empty_container(),
            zone_occupancy=ZoneOccupancyEventsContainer.create_empty_container(),
            motion_status=MotionStatusEventsContainer.create_empty_container()
        )

    # TODO: more advanced validation
    # (1) impossible combinations ...
    # (2) all vehicle ids alive -- must be present in speeds, global motion status updates

class FrameMetadataForEvents(BaseModel):

    # The camera ID (must always be defined).
    camera_id: str

    # The frame ID associated with this event container and the timestamp associated with it.
    # Each of the individual events still carries its own frame ID/timestamp
    # and can, in principle, still define ones that are different from these
    # (although this never occurs in the current implementation).
    #
    # These are meant to be set to None only for the end of processing AND only when
    # no frames have been processed at all (i. e. the pipeline was instantiated,
    # but is then shut down without having processed any frames).
    frame_id: str | None
    frame_ts: datetime | None

    # Whether this container is the first one or the last one, respectively,
    # that is produced by this pipeline.
    # The individual events should have their `truncated` attribute set to `True`,
    # to signal that any periods defined by such events are truncated.
    # - the first container produced by the pipeline
    is_processing_start: bool
    # - the last container produced by the pipeline before a shutdown
    is_processing_end: bool

    def _check_start_and_end_flags_are_consistent(self) -> None:
        # start and end flags cannot both be set
        if self.is_processing_start and self.is_processing_end:
            raise ValueError(
                "Start and end flags cannot be both set to true. "
                "For the end of processing, a separate events container must be produced"
            )

    def _check_frame_data(self) -> None:

        if not self.is_processing_end and (self.frame_id is None or self.frame_ts is None):
            raise ValueError("Frame ID and timestamp cannot be null "
                             "except for the end of processing when no frames were processed")

        both_are_null: bool = self.frame_id is None and self.frame_ts is None
        both_are_nonnull: bool = self.frame_id is not None and self.frame_ts is not None
        if not (both_are_null or both_are_nonnull):
            raise ValueError("Frame ID and frame timestamp must be either both defined or both undefined")

    @model_validator(mode="after")
    def _validate_model(self) -> Self:
        self._check_start_and_end_flags_are_consistent()
        self._check_frame_data()
        return self

class EventsContainer(BaseModel):

    """
    A container for all events produced for a specific frame, with metadata for the frame.
    """

    metadata: FrameMetadataForEvents

    pipeline_steps: SceneEventsWrapper