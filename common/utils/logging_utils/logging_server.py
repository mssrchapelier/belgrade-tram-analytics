from typing import override, Dict, Any
from types import FrameType
from socketserver import StreamRequestHandler, ThreadingTCPServer
from socket import socket as SocketType
import pickle
import logging
from logging import LogRecord, makeLogRecord, Logger, getLogger
import signal
from threading import Thread

from common.settings.constants import LOGGING_SERVER_HOST, LOGGING_SERVER_PORT
from common.utils.networking.length_prefixed_msg import read_length_prefixed_message

LOGGING_SERVER_SHUTDOWN_SIGNAL_POLL_INTERVAL: float = 0.5

class LogRecordStreamHandler(StreamRequestHandler):

    def _get_next_log_record(self) -> LogRecord:
        # Designed to match SocketHandler.makePickle.
        # - receive the length prefix
        socket: SocketType = self.request
        # - receive the message containing the log record.
        log_record_bytes: bytes = read_length_prefixed_message(socket)
        # - unpickle
        log_record_asdict: Dict[str, Any] = pickle.loads(log_record_bytes)
        # - create a log record
        log_record: LogRecord = makeLogRecord(log_record_asdict)
        return log_record

    @override
    def handle(self) -> None:
        while True:
            record: LogRecord = self._get_next_log_record()
            # - handle the record
            logger: Logger = getLogger(record.name)
            if logger.isEnabledFor(record.levelno):
                logger.handle(record)

class LogRecordSocketReceiver(ThreadingTCPServer):

    """
    Listens for log records from different processes and handles them.

    CAUTION: This implementation uses `LogRecordStreamHandler`
    which simply unpickles bytes coming from the socket.
    The senders must be trusted to only send log records.

    DO NOT expose the underlying port to untrusted processes!!!

    TODO: a more secure implementation
    """

    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, *, host: str, port: int) -> None:
        super().__init__((host, port), LogRecordStreamHandler)


def run_logging_server() -> None:
    server: LogRecordSocketReceiver = LogRecordSocketReceiver(
        host=LOGGING_SERVER_HOST, port=LOGGING_SERVER_PORT
    )

    def stop_server(sig_num: int, frame: FrameType | None) -> None:
        # stop serving
        logging.debug("Shutting down logging server ...")
        server.shutdown()
        logging.info("Logging server has been shut down")

    # shutdown and close the socket on SIGTERM, SIGINT
    signal.signal(signal.SIGTERM, stop_server)
    signal.signal(signal.SIGINT, stop_server)

    # the serving method must run in a separate thread
    # in order for the shutdown call from this thread to not deadlock
    server_thread: Thread = Thread(
        target=server.serve_forever,
        kwargs={"poll_interval": LOGGING_SERVER_SHUTDOWN_SIGNAL_POLL_INTERVAL}
    )

    try:
        logging.info(f"Starting logging server (listening on {LOGGING_SERVER_HOST}:{LOGGING_SERVER_PORT})")
        # start serving
        server_thread.start()
        # wait for shutdown
        server_thread.join()
    finally:
        # close the socket
        logging.debug("Closing logging server socket ...")
        server.server_close()
        logging.info("Logging server socket closed")

