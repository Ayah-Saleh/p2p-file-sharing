from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from protocol.handshake import build_handshake, parse_handshake
from protocol.messages import (
    Message,
    MsgType,
    bitfield_has_piece,
    bitfield_set_piece,
    build_have_payload,
    build_piece_payload,
    build_request_payload,
    parse_have_payload,
    parse_piece_payload,
    parse_request_payload,
)


def test_handshake_roundtrip() -> None:
    peer_id = 123456
    data = build_handshake(peer_id)
    assert len(data) == 32
    assert parse_handshake(data) == peer_id


def test_handshake_rejects_wrong_header() -> None:
    valid = build_handshake(7)
    invalid = b"X" + valid[1:]

    try:
        parse_handshake(invalid)
        raise AssertionError("Expected ValueError for invalid header")
    except ValueError:
        pass


def test_message_encode_length_correctness() -> None:
    payload = build_request_payload(42)
    msg = Message(msg_type=MsgType.REQUEST, payload=payload)
    encoded = msg.encode()

    declared_length = int.from_bytes(encoded[:4], "big")
    assert declared_length == 1 + len(payload)


def test_payload_roundtrips() -> None:
    piece_index = 9

    have_payload = build_have_payload(piece_index)
    assert parse_have_payload(have_payload) == piece_index

    request_payload = build_request_payload(piece_index)
    assert parse_request_payload(request_payload) == piece_index

    piece_data = b"hello-piece"
    piece_payload = build_piece_payload(piece_index, piece_data)
    parsed_index, parsed_data = parse_piece_payload(piece_payload)
    assert parsed_index == piece_index
    assert parsed_data == piece_data


def test_bitfield_bit_order() -> None:
    bitfield = bytearray(2)

    bitfield_set_piece(bitfield, 0)
    bitfield_set_piece(bitfield, 7)
    bitfield_set_piece(bitfield, 8)

    assert bitfield[0] == 0b10000001
    assert bitfield[1] == 0b10000000

    assert bitfield_has_piece(bytes(bitfield), 0)
    assert bitfield_has_piece(bytes(bitfield), 7)
    assert bitfield_has_piece(bytes(bitfield), 8)
    assert not bitfield_has_piece(bytes(bitfield), 1)


def run_all_tests() -> None:
    test_handshake_roundtrip()
    test_handshake_rejects_wrong_header()
    test_message_encode_length_correctness()
    test_payload_roundtrips()
    test_bitfield_bit_order()
    print("All protocol tests passed")


if __name__ == "__main__":
    run_all_tests()
