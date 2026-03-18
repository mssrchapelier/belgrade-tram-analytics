from datetime import datetime, timezone
from hashlib import md5
from typing import override, AsyncIterator

import numpy as np
from numpy import int64
from numpy.random import Generator, default_rng

from tram_analytics.v2.pipeline._base.ingestion_stage.adapters.output_adapters.repository.write_repo import (
    BaseIngestionStageWriteRepo
)
from tram_analytics.v2.pipeline._base.ingestion_stage.base_ingestion_stage import (
    SessionIDSeqNum
)
from tram_analytics.v2.pipeline._mock._base.service.ingestion_stage.adapters.output_adapters.mq.to_pipeline import (
    BaseIngestionMQToPipeline
)
from tram_analytics.v2.pipeline._mock._base.service.ingestion_stage.base_ingestion_stage import (
    BaseIngestionStage, BaseIngestionStageConfig
)
from tram_analytics.v2.pipeline._mock.common.dto.data_models import NumberObservation, EmittedNumberAsStored
from tram_analytics.v2.pipeline._mock.common.dto.messages import (
    FrameJobInProgressMessage, IngestionDroppedItemMessage, CriticalErrorMessage
)
from tram_analytics.v2.pipeline._mock.mock_src_stream import get_mock_input_generator


class NumberEmitterStageConfig(BaseIngestionStageConfig):
    pass

# accepts: (number, frame_ts)
class NumberEmitterStage(
    BaseIngestionStage[
        NumberObservation, EmittedNumberAsStored, NumberEmitterStageConfig
    ]
):

    def __init__(self, *,
                 # a seed for RNG for the mock setup, for repeatability
                 seed: int,
                 # --- for the base constructor ---
                 config: NumberEmitterStageConfig,
                 read_repo: BaseIngestionStageWriteRepo[EmittedNumberAsStored],
                 write_repo: BaseIngestionMQToPipeline) -> None:
        super().__init__(config=config,
                         read_repo=read_repo,
                         write_repo=write_repo)
        self._rng: Generator = default_rng(seed)

    @override
    def _get_new_source_stream(self) -> AsyncIterator[NumberObservation]:
        return get_mock_input_generator(
            seed=self._rng.integers(low=1, high=100000).item()
        )

    @staticmethod
    def _get_current_timestamp() -> float:
        # the current time, UTC, POSIX seconds
        return datetime.now(tz=timezone.utc).timestamp()

    @override
    def _build_session_end_message_from_last_emitted(
            self, msg_for_last_emitted: FrameJobInProgressMessage
    ) -> FrameJobInProgressMessage:
        new_msg: FrameJobInProgressMessage = FrameJobInProgressMessage(
            camera_id=msg_for_last_emitted.camera_id,
            frame_id=msg_for_last_emitted.frame_id,
            frame_ts=msg_for_last_emitted.frame_ts,
            session_id=msg_for_last_emitted.session_id,
            seq_num=msg_for_last_emitted.seq_num,
            # session end
            is_session_end=True,
            # no expiration
            expires=None
        )
        return new_msg

    @override
    def _must_end_prev_session(
            self, *, input_item: NumberObservation, last_success_msg: FrameJobInProgressMessage
    ) -> bool:
        # mock: for now, just never reset sessions
        return False

    def _get_frame_id(self):
        """
        Generate the frame ID as the hash of a random number generated with the use of the RNG set up at init time.
        Used for repeatability in this mock setup (non-mocks use UUIDs).
        """
        # generate a random int64
        random_num: int = self._rng.integers(low=np.iinfo(int64).min,
                                             high=np.iinfo(int64).max).item()
        as_bytes: bytes = random_num.to_bytes(length=np.iinfo(int64).bits,
                                              byteorder="little",
                                              signed=True)
        hash_str: str = md5(as_bytes).hexdigest()
        return hash_str

    @override
    async def _process(self, input_item: NumberObservation) -> EmittedNumberAsStored:
        # this is a mock example; in frame ingestion, both the consumed and the emitted item
        # must incorporate more information (frame PTS, etc.)
        frame_id: str = self._get_frame_id()

        session_state_details: SessionIDSeqNum | None = self._session_id_seq_num_updater.get_current()
        assert session_state_details is not None

        return EmittedNumberAsStored(camera_id=self._config.camera_id,
                                     frame_id=frame_id,
                                     number=input_item.number,
                                     frame_ts=input_item.frame_ts,
                                     session_id=session_state_details.session_id,
                                     seq_num=session_state_details.seq_num)

    @override
    def _create_message_success(self, output_item: EmittedNumberAsStored) -> FrameJobInProgressMessage:
        # the time to live for the message
        assert self._session_id_seq_num_updater is not None
        identifiers: SessionIDSeqNum | None = self._session_id_seq_num_updater.get_current()
        assert identifiers is not None
        message_ttl: float | None = self._config.message_ttl
        expires: float | None = (
            self._get_current_timestamp() + message_ttl
            if message_ttl is not None
            else None
        )
        return FrameJobInProgressMessage(camera_id=output_item.camera_id,
                                         frame_id=output_item.frame_id,
                                         frame_ts=output_item.frame_ts,
                                         session_id=identifiers.session_id,
                                         seq_num=identifiers.seq_num,
                                         is_session_end=False,
                                         expires=expires)

    @override
    def _get_frame_id_from_message(self, msg: FrameJobInProgressMessage) -> str:
        return msg.frame_id

    @override
    def _get_dropped_job_mq_message(self, description: str) -> IngestionDroppedItemMessage:
        return IngestionDroppedItemMessage(details=description)

    @override
    def _get_critical_error_mq_message(self, exc: Exception) -> CriticalErrorMessage:
        return CriticalErrorMessage(details=type(exc).__name__)