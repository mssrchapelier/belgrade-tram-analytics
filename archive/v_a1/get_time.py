from datetime import datetime, timezone

def get_current_timestamp():
    """
    Returns the current timestamp in ISO 8601 format with the timezone designator.
    """
    return datetime.now(tz=timezone.utc).isoformat()