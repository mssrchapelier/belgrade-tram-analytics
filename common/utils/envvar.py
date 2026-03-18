import os

# helpers for loading environment variables

def load_envvar_as_str(key: str) -> str:
    value: str | None = os.environ.get(key)
    if value is None:
        raise RuntimeError(f"Could not get environment variable: {key}")
    return value

def load_envvar_as_int(key: str) -> int:
    value_str: str = load_envvar_as_str(key)
    try:
        value: int = int(value_str)
        return value
    except ValueError as exc:
        raise ValueError(f"Invalid environment variable: {key}") from exc
