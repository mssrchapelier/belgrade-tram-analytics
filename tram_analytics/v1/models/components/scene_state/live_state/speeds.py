from pydantic import BaseModel, NonNegativeFloat

# --- speeds ---

class SpeedStats(BaseModel):
    # aggregated statistics wrt the vehicle's lifetime
    max_ms: NonNegativeFloat | None
    mean_ms: NonNegativeFloat | None
    median_ms: NonNegativeFloat | None


class SpeedStatsWithCurrent(SpeedStats):
    current_ms: NonNegativeFloat | None


class BaseSpeedWrapper[SpeedContainer: SpeedStats](BaseModel):
    raw: SpeedContainer
    smoothed: SpeedContainer


class LifetimeSpeeds(BaseSpeedWrapper[SpeedStatsWithCurrent]):
    # includes the current speed
    pass


class InZoneSpeeds(BaseSpeedWrapper[SpeedStats]):
    # does not include the current speed (to avoid duplication)
    pass
