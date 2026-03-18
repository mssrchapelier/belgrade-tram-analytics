from typing import Self, TypeAlias
from dataclasses import dataclass

from common.utils.time_utils import get_datetime_utc


@dataclass(frozen=True, slots=True, kw_only=True)
class FrameMessage:
    """
    The format of messages to be passed for every item successfully processed by the upstream producer,
    to be fetched by the downstream consumer.
    """
    session_id: str
    # The sequence number of the frame (for this camera and this session).
    # Is meant to be strictly increasing.
    seq_num: int
    frame_id: str
    # The PTS of the frame, in POSIX seconds.
    frame_pts: float
    # The timestamp of the message's creation time.
    message_ts: float

    @classmethod
    def from_message(cls, msg: Self) -> Self:
        # copies msg, but inserts the current timestamp as message_ts
        cur_time_utc_posix: float = get_datetime_utc().timestamp()
        return cls(session_id=msg.session_id,
                   seq_num=msg.seq_num,
                   frame_id=msg.frame_id,
                   frame_pts=msg.frame_pts,
                   message_ts=cur_time_utc_posix)

# just the frame ID for now
RealtimeRetrievalRequest: TypeAlias = str
