import struct
from socket import socket as SocketType

class StreamEndedError(Exception):
    pass

class StreamTruncatedError(Exception):
    pass

def recv_exact_into(sock: SocketType, *, n: int) -> bytearray:
    """
    Reads exactly `n` bytes from `sock` into a preallocated bytearray.
    Returns the bytearray.
    """
    # preallocate a bytearray to write into
    buffer: bytearray = bytearray(n)
    view: memoryview = memoryview(buffer)
    bytes_read: int = 0

    while bytes_read < n:
        num_received: int = sock.recv_into(view[bytes_read:])
        if num_received == 0:
            if bytes_read == 0:
                raise StreamEndedError("Stream ended before any bytes were read")
            raise StreamTruncatedError(f"Stream ended after {bytes_read} bytes (expected {n} bytes)")
        bytes_read += num_received

    return buffer

def recv_exact(sock: SocketType, *, n: int) -> bytes:
    """
    A convenience wrapper for `recv_exact_into` returning `bytes` rather than a `bytearray`.
    """
    return bytes(recv_exact_into(sock, n=n))

def read_length_prefixed_message(sock: SocketType) -> bytes:
    """
    Read and decode the length prefix (an unsigned 32-bit integer, big-endian),
    then read the message of this length and return as bytes.
    """
    msg_length_chunk: bytes = recv_exact(sock, n=4)
    msg_length: int = struct.unpack(">L", msg_length_chunk)[0]
    msg: bytes = recv_exact(sock, n=msg_length)
    return msg
