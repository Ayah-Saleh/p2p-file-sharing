import struct
from dataclasses import dataclass
from enum import IntEnum


class MsgType(IntEnum):
    CHOKE = 0
    UNCHOKE = 1
    INTERESTED = 2
    NOT_INTERESTED = 3
    HAVE = 4
    BITFIELD = 5
    REQUEST = 6
    PIECE = 7


@dataclass
class Message:
    msg_type: MsgType
    payload: bytes = b""

    def encode(self) -> bytes:
        # the body is one type byte followed by the payload bytes
        body = bytes([int(self.msg_type)]) + self.payload
        length = len(body)
        return struct.pack(">I", length) + body


_EMPTY_PAYLOAD_TYPES = {
    MsgType.CHOKE,
    MsgType.UNCHOKE,
    MsgType.INTERESTED,
    MsgType.NOT_INTERESTED,
}


def decode_from_body(body: bytes) -> Message:
    # body here means only type plus payload, not the 4 byte length field
    if not body:
        raise ValueError("message body cannot be empty")

    msg_type_val = body[0]
    payload = body[1:]

    try:
        msg_type = MsgType(msg_type_val)
    except ValueError as exc:
        raise ValueError(f"unknown message type: {msg_type_val}") from exc

    if msg_type in _EMPTY_PAYLOAD_TYPES and payload:
        raise ValueError(f"message type {msg_type.name} must not have payload")

    if msg_type in (MsgType.HAVE, MsgType.REQUEST) and len(payload) != 4:
        raise ValueError(f"message type {msg_type.name} payload must be 4 bytes")

    if msg_type == MsgType.PIECE and len(payload) < 4:
        raise ValueError("PIECE payload must include 4-byte piece index")

    return Message(msg_type=msg_type, payload=payload)


def build_have_payload(piece_index: int) -> bytes:
    return _pack_piece_index(piece_index)


def parse_have_payload(payload: bytes) -> int:
    return _unpack_piece_index(payload, "HAVE")


def build_request_payload(piece_index: int) -> bytes:
    return _pack_piece_index(piece_index)


def parse_request_payload(payload: bytes) -> int:
    return _unpack_piece_index(payload, "REQUEST")


def build_piece_payload(piece_index: int, piece_data: bytes) -> bytes:
    if not isinstance(piece_data, (bytes, bytearray)):
        raise ValueError("piece_data must be bytes-like")
    return _pack_piece_index(piece_index) + bytes(piece_data)


def parse_piece_payload(payload: bytes) -> tuple[int, bytes]:
    # split the piece payload into the piece index and the raw piece bytes
    if len(payload) < 4:
        raise ValueError("PIECE payload must be at least 4 bytes")
    piece_index = struct.unpack(">I", payload[:4])[0]
    piece_data = payload[4:]
    return piece_index, piece_data


def bitfield_has_piece(bitfield: bytes, piece_index: int) -> bool:
    if piece_index < 0:
        raise ValueError("piece_index must be non-negative")

    byte_index = piece_index // 8
    bit_offset = piece_index % 8

    if byte_index >= len(bitfield):
        return False

    mask = 1 << (7 - bit_offset)
    return (bitfield[byte_index] & mask) != 0


def bitfield_set_piece(bitfield: bytearray, piece_index: int) -> None:
    if piece_index < 0:
        raise ValueError("piece_index must be non-negative")

    byte_index = piece_index // 8
    bit_offset = piece_index % 8

    if byte_index >= len(bitfield):
        raise ValueError("piece_index out of bitfield range")

    mask = 1 << (7 - bit_offset)
    bitfield[byte_index] |= mask


def _pack_piece_index(piece_index: int) -> bytes:
    if not (0 <= piece_index <= 0xFFFFFFFF):
        raise ValueError("piece_index must be in range [0, 2^32-1]")
    return struct.pack(">I", piece_index)


def _unpack_piece_index(payload: bytes, name: str) -> int:
    if len(payload) != 4:
        raise ValueError(f"{name} payload must be exactly 4 bytes")
    return struct.unpack(">I", payload)[0]
