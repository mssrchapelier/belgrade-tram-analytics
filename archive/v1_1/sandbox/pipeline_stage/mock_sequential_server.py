import zmq
from zmq import Context, Socket
import time
import logging
from logging import Logger

from numpy.random import Generator, default_rng

from archive.v1_1.sandbox.pipeline_stage._test import _get_logger

def serialise_int(n: int) -> bytes:
    return n.to_bytes(length=64, byteorder="big", signed=True)

def deserialise_int(n: bytes) -> int:
    return int.from_bytes(n, byteorder="big", signed=True)

class MockSequentialServer:

    """
    Simulates a server with a synchronous endpoint performing "work"
    (waiting a random amount of time before emitting a response)
    that must be updated strictly sequentially.
    """

    def __init__(self, *, min_wait: float, max_wait: float,
                 seed: int, address: str, logger: Logger) -> None:
        self._logger: Logger = logger

        # simulate random processing times
        self._min_wait: float = min_wait
        self._max_wait: float = max_wait

        self._running_sum: int = 0

        self._rng: Generator = default_rng(seed)

        self._address: str = address


    def _get_random_processing_time(self) -> float:
        return self._rng.uniform(low=self._min_wait, high=self._max_wait)

    def update(self, inputs: int) -> int:
        processing_time: float = self._get_random_processing_time()
        time.sleep(processing_time)

        self._running_sum += inputs
        return self._running_sum

    def run(self):
        with (
            zmq.Context() as ctx, ctx.socket(socket_type=zmq.REP) as socket
        ): # type: Context[Socket[bytes]], Socket[bytes]
            socket.bind(addr=self._address)
            logging.info(f"Running server on {self._address} ...")
            while True:
                # blocking, sequential
                msg_bytes: bytes = socket.recv()
                input_item: int = deserialise_int(msg_bytes)
                self._logger.debug(f"Server received: {input_item}")
                result: int = self.update(input_item)
                result_bytes: bytes = serialise_int(result)
                self._logger.debug(f"Server sending: {result}")
                socket.send(result_bytes)

def _build_server() -> MockSequentialServer:
    server_address: str = "ipc:///tmp/test_pipeline_stage"
    log_path: str = "REDACTED/2026_02_16_mock_sequential_pipeline_SERVER_log.txt"
    logger: Logger = _get_logger(log_path)
    seed: int = 187392
    server: MockSequentialServer = MockSequentialServer(min_wait=0.7, max_wait=0.7, seed=seed,
                                                        address=server_address, logger=logger)
    return server

def _run_server():
    server: MockSequentialServer = _build_server()
    server.run()

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    _run_server()