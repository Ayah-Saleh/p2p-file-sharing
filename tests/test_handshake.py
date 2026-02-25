from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from protocol.handshake import build_handshake, parse_handshake


def test_handshake_roundtrip() -> None:
    peer_id = 123456
    data = build_handshake(peer_id)
    assert len(data) == 32
    assert parse_handshake(data) == peer_id


def test_invalid_header_rejection() -> None:
    valid = build_handshake(7)
    invalid = b"X" + valid[1:]

    try:
        parse_handshake(invalid)
        raise AssertionError("Expected ValueError for invalid header")
    except ValueError:
        # Any ValueError is acceptable; we only care that it is rejected.
        pass


def test_invalid_length_rejection() -> None:
    valid = build_handshake(7)
    too_short = valid[:-1]

    try:
        parse_handshake(too_short)
        raise AssertionError("Expected ValueError for invalid length")
    except ValueError:
        # Any ValueError is acceptable; we only care that it is rejected.
        pass


if __name__ == "__main__":
    test_handshake_roundtrip()
    test_invalid_header_rejection()
    test_invalid_length_rejection()
    print("Handshake tests passed")
