from pydantic import BaseModel, NonNegativeFloat, Field

# --- speeds ---

class SpeedStats(BaseModel):
    # aggregated statistics wrt the vehicle's lifetime
    max_ms: NonNegativeFloat | None = Field(
        description="Maximum speed value, in metres per second."
    )
    mean_ms: NonNegativeFloat | None = Field(
        description="Mean speed value, in metres per second."
    )
    median_ms: NonNegativeFloat | None = Field(
        description="Median speed value (across all observations), in metres per second."
    )


class SpeedStatsWithCurrent(SpeedStats):
    current_ms: NonNegativeFloat | None = Field(
        description="Current speed, in metres per second."
    )


class BaseSpeedWrapper[SpeedContainer: SpeedStats](BaseModel):
    raw: SpeedContainer = Field(
        description="Raw speeds, i. e. those without smoothing applied."
    )
    smoothed: SpeedContainer = Field(
        description="Smoothed speeds."
    )


class LifetimeSpeeds(BaseSpeedWrapper[SpeedStatsWithCurrent]):
    # includes the current speed
    """
    Lifetime speed statistics for a given vehicle.
    """
    pass


class InZoneSpeeds(BaseSpeedWrapper[SpeedStats]):
    # does not include the current speed (to avoid duplication)
    """
    Speed statistics for a given vehicle for the duration of its presence in a given zone.
    """
    pass
