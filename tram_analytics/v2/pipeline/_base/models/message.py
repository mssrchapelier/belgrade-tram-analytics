from abc import ABC
from asyncio import Future
from dataclasses import dataclass, field
from typing import Self, NamedTuple, override

from pydantic import BaseModel


class NegativeAcknowledgementException(Exception):
    pass

class WorkerJobID(NamedTuple):
    """
    Represents a unique update for any worker for idempotency purposes
    (meaningful mostly for stateful workers, but is used with all, at least to not repeat work already done):
    the frame ID, and whether this update is for the session's end
    (in which case certain stateful workers will need to update their state for the last frame ID seen,
    e. g. with the scene state updater which needs to emit and persist end events for the periods it is tracking).

    Idempotency wrt this combination is managed at stage level,
    although the workers themselves can do so too if needed.
    """
    frame_id: str
    is_session_end: bool

    @override
    def __str__(self) -> str:
        return f"frame ID: {self.frame_id}, session end: {self.is_session_end}"

class BaseFrameMessage(BaseModel, ABC):
    # The camera (stream) identifier for the source of the frame.
    # Identifies the per-camera pipeline in which this message is to be processed.
    camera_id: str
    # The unique identifier for the video frame associated with this message.
    frame_id: str
    # The timestamp associated with the frame, in POSIX seconds.
    # Meant to be set from an aware datetime object set to UTC (or derived from a known UTC-based timestamp).
    frame_ts: float
    # The session identifier for the frame ingestion session associated with this message.
    # Must increase monotonically.
    session_id: int
    # The frame's sequence number inside the session. Must increase monotonically.
    seq_num: int
    # When the ingestion stage ends the session, it must emit a message with the last parameters,
    # but with `is_session_end` set to `True`. This is used to signal stateful workers to reset their state,
    # and also emit outputs if they are so configured
    # (currently, the scene state processor, which will emit period end events in this case).
    is_session_end: bool

    def get_job_id(self) -> WorkerJobID:
        return WorkerJobID(frame_id=self.frame_id,
                           is_session_end=self.is_session_end)

class BaseFrameJobInProgressMessage(BaseFrameMessage, ABC):
    """
    A message used for any frame that is "in progress" in the pipeline (i. e. not dropped, or not YET dropped).
    Can be used as both the input and the output message, for any frame that is not being dropped.
    """
    # The expiration timestamp for this message, in POSIX seconds.
    # Usage: for the message queue broker to discard messages corresponding to stale frames.
    # Meant to be calculated once based on the frame's timestamp,
    # and then propagated through all downstream messages created for that frame.
    #
    # NOTE: For messages with `is_session_end=True`,
    # it might make sense to configure them to persist longer, or to not expire at all.
    expires: float | None

class BaseIngestionDroppedItemMessage(BaseModel, ABC):
    # Not containing the frame ID because none might have been assigned to the item
    # yet there is a need to report such instances.
    # TODO: perhaps assign the frame ID early on in the ingestion module to report it here
    details: str

class BaseDroppedJobMessage(BaseModel, ABC):
    """
    Indicates that the frame was dropped by the stage 
    and that an output message for downstream stages will not be published.
    Can be published to e. g. a queue to which a health monitoring module is subscribed.
    """
    job_id: WorkerJobID
    details: str

class BaseCriticalErrorMessage(BaseModel, ABC):
    # Not containing a frame ID because a critical error
    # might not always be associated with the processing of any specific item.
    # The using modules are currently responsible for providing any details in the `details` field.
    details: str

@dataclass(frozen=True, slots=True, kw_only=True)
class MessageWithAckFuture[MsgT]:
    """
    A container holding: (1) a message to be passed for processing;
    (2) the future to be fulfilled once the message is considered to be ready to be acknowledged.
    Motivation: Implementing a pipeline-like message handler
    that chooses when to signal that the message can be acknowledged
    based on its own processing logic, -- separately from the message queue consumer
    that merely sends messages over to the handler for processing.
    """
    # the message itself
    message: MsgT
    # The future to fulfil once the message is ready to be acknowledged.
    # Set to `None` for positive acknowledgement (ack).
    # Set an exception to this future for negative acknowledgement (nack).
    ack_future: Future[None] = field(default_factory=Future)

class WorkerInputMessageWrapper[InputMsgT](NamedTuple):
    """
    Wraps a message along with the frame ID from a stage to a worker.
    """
    # An output wrapper IS needed because it can hold either a success message or an exception
    # but a frame ID is needed in any case.
    # The corresponding input container (this one), also with a frame ID, was introduced for consistency,
    # but perhaps is not needed.
    # TODO: possibly remove (see rationale above)
    job_id: WorkerJobID
    inputs_msg: InputMsgT

@dataclass(frozen=True, slots=True, kw_only=True)
class WorkerOutputMessageWrapper[OutputMsgT]:
    """
    A wrapper for worker outputs that holds:
    - the frame ID (for cross-reference);
    - a future containing either the output or the exception
    raised during the processing of this item.
    Moivation:
    (1) To allow for exception handling on a per-item basis.
    (2) To enable cross-referencing the frame IDs of the outputs,
    especially because whether the outputs preserve the order of the inputs
    depends on the processor's implementation.
    """

    job_id: WorkerJobID
    _outcome: Future[OutputMsgT] = field(default_factory=Future)

    def set_result(self, result: OutputMsgT) -> Self:
        self._outcome.set_result(result)
        return self

    def set_exception(self, exc: Exception) -> Self:
        self._outcome.set_exception(exc)
        return self

    async def get_output(self) -> OutputMsgT:
        # will either return `OutputT`, or raise a `ProcessorException`
        return await self._outcome
