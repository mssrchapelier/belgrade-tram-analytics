from pydantic import BaseModel, PositiveInt, NonNegativeInt

from tram_analytics.v1.models.common_types import SpeedDisplayUnit


class LiveStateRendererConfig(BaseModel):
    uuid_truncation_length: PositiveInt
    speed_unit: SpeedDisplayUnit
    speed_decimal_places: NonNegativeInt
