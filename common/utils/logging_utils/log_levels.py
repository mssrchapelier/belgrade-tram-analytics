import logging
from enum import StrEnum, auto
from typing import Dict

class LoggingLevelSetting(StrEnum):
    """
    An enum for default logging levels. Used to enable convenient setting
    of the logging level through config/envvar whilst validating the value.
    """
    NOTSET = auto()
    DEBUG = auto()
    INFO = auto()
    WARNING = auto()
    CRITICAL = auto()
    ERROR = auto()

_LOGGING_LEVEL_STR_TO_INT: Dict[LoggingLevelSetting, int] = {
    LoggingLevelSetting.NOTSET: logging.NOTSET,
    LoggingLevelSetting.DEBUG: logging.DEBUG,
    LoggingLevelSetting.INFO: logging.INFO,
    LoggingLevelSetting.WARNING: logging.WARNING,
    LoggingLevelSetting.CRITICAL: logging.CRITICAL,
    LoggingLevelSetting.ERROR: logging.ERROR
}

def get_logging_level_from_setting(setting: LoggingLevelSetting) -> int:
    try:
        return _LOGGING_LEVEL_STR_TO_INT[setting]
    except KeyError as exc:
        raise ValueError(f"Unknown logging level setting: {setting}") from exc
