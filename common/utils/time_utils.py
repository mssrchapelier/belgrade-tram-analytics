from datetime import datetime, timezone


def datetime_to_utc_posix(ts: datetime) -> float:
    """
    Convert `ts` to POSIX seconds, setting the time zone to UTC before the conversion.
    """
    return ts.replace(tzinfo=timezone.utc).timestamp()


def posix_to_utc_datetime(ts: float) -> datetime:
    """
    Convert POSIX seconds to an (aware) datetime object, with its timezone set to UTC.
    """
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def get_datetime_utc() -> datetime:
    return datetime.now(tz=timezone.utc)
