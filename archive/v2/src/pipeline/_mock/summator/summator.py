import time

from pydantic import BaseModel, NonNegativeFloat

from tram_analytics.v2.pipeline._mock.common.dto.data_models import Square, SumSquares

class SummatorConfig(BaseModel):
    delay: NonNegativeFloat

class Summator:

    def __init__(self, config: SummatorConfig) -> None:
        self._config: SummatorConfig = config

        self._prev_frame_id: str | None = None

        self._counter: int = 0

    def process(self, square: Square) -> SumSquares:
        cur_frame_id: str = square.frame_id
        time.sleep(self._config.delay)
        self._counter += square.square

        self._prev_frame_id = cur_frame_id

        return SumSquares(frame_id=cur_frame_id,
                          sum=self._counter)

    def process_for_session_end(self) -> SumSquares | None:
        # returns: the last frame ID and (-counter)
        # resets the counter
        if self._prev_frame_id is None:
            return None
        to_return: SumSquares = SumSquares(frame_id=self._prev_frame_id,
                                           sum= - self._counter)
        # reset the counter
        self._counter = 0
        return to_return