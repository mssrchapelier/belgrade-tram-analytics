import time

from pydantic import BaseModel, NonNegativeFloat

from tram_analytics.v2.pipeline._mock.common.dto.data_models import EmittedNumber, Square


class SquarerConfig(BaseModel):
    delay: NonNegativeFloat

class Squarer:

    def __init__(self, config: SquarerConfig) -> None:
        self._config: SquarerConfig = config

    def process(self, emitted_num: EmittedNumber) -> Square:
        time.sleep(self._config.delay)
        return Square(frame_id=emitted_num.frame_id,
                      square=emitted_num.number ** 2)