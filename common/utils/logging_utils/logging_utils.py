from typing import TextIO, List, Type
import logging
from logging import Logger, Handler, StreamHandler, FileHandler, Formatter
from logging.handlers import SocketHandler
import time
import sys
from pathlib import Path

FORMAT: str = "%(asctime)s.%(msecs)03d | %(name)s | %(levelname)s | %(message)s"
DATEFMT: str = "%H:%M:%S"

def configure_global_logging(*, log_path: str | None = None, level: int = logging.DEBUG) -> None:
    formatter: Formatter = _get_formatter()
    handlers: List[Handler] = list()
    # stderr
    handlers.append(_get_stderr_handler(formatter, level))
    # file (single for all logging in this setup)
    if log_path is not None:
        parent_dir: Path = Path(log_path).parent
        parent_dir.mkdir(parents=True, exist_ok=True)
        handlers.append(_get_file_handler(log_path, formatter))
    logging.basicConfig(format=FORMAT, datefmt=DATEFMT, level=level,
                        handlers=handlers)

def configure_global_logging_to_tcp_socket(
        *, host: str, port: int,
        level: int = logging.INFO,
) -> None:
    root_logger: Logger = logging.getLogger()

    if len(root_logger.handlers) > 0:
        root_logger.handlers.clear()

    root_logger.setLevel(level)

    formatter: Formatter = _get_formatter()
    # stderr
    root_logger.addHandler(_get_stderr_handler(formatter, level))
    # to socket
    socket_handler: SocketHandler = SocketHandler(host, port)
    socket_handler.setLevel(level)
    root_logger.addHandler(socket_handler)

def get_logger_name_for_object(obj: object) -> str:
    return "{} > {}".format(obj.__class__.__module__,
                             obj.__class__.__name__)

def _get_formatter() -> Formatter:
    formatter: Formatter = Formatter(fmt=FORMAT, datefmt=DATEFMT)
    # set time to UTC
    formatter.converter = time.gmtime
    return formatter

def _get_stderr_handler(formatter: Formatter, level: int) -> StreamHandler[TextIO]:
    handler: StreamHandler[TextIO] = StreamHandler(sys.stderr)
    handler.setLevel(level)
    handler.setFormatter(formatter)
    return handler

def _get_file_handler(log_path: str, formatter: Formatter) -> FileHandler:
    handler: FileHandler = FileHandler(log_path, mode="w", encoding="utf8")
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(formatter)
    return handler

def configure_default_logger(name: str,
                             *, log_path: str | None = None,
                             level: int = logging.DEBUG) -> Logger:
    """
    A logger that writes to stderr and the provided file path (if provided)
    """
    logger: Logger = logging.getLogger(name)

    logger.setLevel(level)
    formatter: Formatter = _get_formatter()

    stderr_stream_handler: StreamHandler[TextIO] = _get_stderr_handler(formatter, level)
    logger.addHandler(stderr_stream_handler)

    if log_path is not None:
        file_handler: FileHandler = _get_file_handler(log_path, formatter)
        logger.addHandler(file_handler)

    logger.info("NOTE: All timestamps are in UTC")

    return logger