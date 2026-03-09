import struct

HANDSHAKE_HEADER = b"P2PFILESHARINGPROJ"
HANDSHAKE_ZEROS = b"\x00" * 10
HANDSHAKE_LEN = 32


def build_handshake(peer_id: int) -> bytes:
    if not (0 <= peer_id <= 0xFFFFFFFF):
        raise ValueError("peer_id must be in range [0, 2^32-1]")
    return HANDSHAKE_HEADER + HANDSHAKE_ZEROS + struct.pack(">I", peer_id)


def parse_handshake(data: bytes) -> int:
    if len(data) != HANDSHAKE_LEN:
        raise ValueError("handshake must be exactly 32 bytes")

    header = data[:18]
    zeros = data[18:28]
    peer_id_bytes = data[28:32]

    if header != HANDSHAKE_HEADER:
        raise ValueError("invalid handshake header")
    if zeros != HANDSHAKE_ZEROS:
        raise ValueError("invalid handshake zero bytes")

    return struct.unpack(">I", peer_id_bytes)[0]
