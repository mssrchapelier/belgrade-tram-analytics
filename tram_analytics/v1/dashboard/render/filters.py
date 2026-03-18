from datetime import datetime, timedelta
from typing import Dict, Callable, Any, List

from tram_analytics.v1.models.common_types import (
    SpeedDisplayUnit, get_speed_unit_str as imported_get_speed_unit_str, convert_speed
)

# "03 May 2026, 07:05:00.200000"
# DATETIME_FORMAT: str = "%d %b %Y, %H:%M:%S.%f"
# "03 May 2026, 07:05:00"
DATETIME_FORMAT: str = "%d %b %Y, %H:%M:%S"

# registry to use with jinja environment
FILTERS: Dict[str, Callable[..., Any]] = dict()

def register(name: str | None = None) -> Callable[..., Any]:
    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        func_name: str = name if name is not None else func.__name__
        FILTERS[func_name] = func
        return func
    return decorator

@register("truncate_uuid")
def truncate_uuid(uuid_str: str, length: int) -> str:
    """
    Truncate a UUID to the specified length.
    """
    return uuid_str[:length]

@register("get_datetime_str")
def get_datetime_str(ts: datetime | None) -> str:
    """
    Export a datetime object according to the specified global format.
    """
    return ts.strftime(DATETIME_FORMAT) if ts is not None else "n/a"

@register("get_timedelta_str")
def get_timedelta_str(ts: datetime | None, ref_ts: datetime | None) -> str:
    """
    Compute the difference between the two timestamps
    and return a human-readable string describing the delta.
    """
    if ts is None or ref_ts is None:
        return "n/a"

    delta: timedelta = ref_ts - ts

    out_str_elems: List[str] = list()
    if delta.days != 0:
        out_str_elems.append(f"{delta.days} days")
    total_s: int = delta.seconds
    hours, hrs_remainder_s = divmod(total_s, 3600) # type: int, int
    if hours > 0:
        out_str_elems.append(f"{hours} h")
    minutes, min_remainder_s = divmod(hrs_remainder_s, 60) # type: int, int
    if minutes > 0:
        out_str_elems.append(f"{minutes} m")
    out_str_elems.append(f"{min_remainder_s} s")

    out_str: str = " ".join(out_str_elems)
    return out_str

@register("get_speed_value_str")
def get_speed_value_str(
        speed_ms: float | None,
        unit: SpeedDisplayUnit,
        decimal_places: int
) -> str:
    """
    Convert speed to the unit used and return a formatted string (without the unit name).
    """
    speed_converted: float | None = convert_speed(speed_ms, unit)
    speed_format_str: str = f"{{:.{decimal_places}f}}" if speed_converted is not None else "n/a"
    formatted: str = speed_format_str.format(speed_converted)
    return formatted

@register("get_speed_unit_str")
def get_speed_unit_str(unit: SpeedDisplayUnit) -> str:
    return imported_get_speed_unit_str(unit)

@register("chain_vehicle_ids")
def chain_vehicle_ids(vehicle_ids: List[str], truncation_length: int) -> str:
    """
    Join vehicle IDs into one string (while truncating).
    """
    return ", ".join(
        truncate_uuid(v_id, length=truncation_length)
        for v_id in vehicle_ids
    )