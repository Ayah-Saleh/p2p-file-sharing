import struct
import socket

from protocol.messages import Message, decode_from_body


def recv_exact(sock: socket.socket, n: int) -> bytes:
    if n < 0:
        raise ValueError("n must be non-negative")

    chunks = bytearray()
    while len(chunks) < n:
        chunk = sock.recv(n - len(chunks))
        if not chunk:
            raise ConnectionError("socket closed while receiving data")
        chunks.extend(chunk)
    return bytes(chunks)


def recv_message(sock: socket.socket) -> Message:
    # read the length first so we know exactly how many bytes to read next
    length_prefix = recv_exact(sock, 4)
    body_length = struct.unpack(">I", length_prefix)[0]

    if body_length <= 0:
        raise ValueError("message length must be at least 1")

    body = recv_exact(sock, body_length)
    return decode_from_body(body)


def send_message(sock: socket.socket, msg: Message) -> None:
    # sendall keeps writing until the full framed message is sent
    sock.sendall(msg.encode())
