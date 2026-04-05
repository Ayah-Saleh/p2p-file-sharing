from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from connection_manager.config import CommonConfig, PeerInfo
from connection_manager.protocol_node import ProtocolPeerNode
from protocol.messages import Message, MsgType, parse_have_payload, parse_request_payload


class DummyLogger:
    def __init__(self) -> None:
        self.events: list[tuple] = []

    def log_tcp_connection_to_peer(self, other_id):
        self.events.append(("tcp_to", other_id))

    def log_tcp_connection_from_peer(self, other_id):
        self.events.append(("tcp_from", other_id))

    def change_preferred_neighbors(self, preferred_neighbors):
        self.events.append(("preferred", tuple(preferred_neighbors)))

    def change_optimistic_unchoked_neighbors(self, optimistic_neighbor_id):
        self.events.append(("optimistic", optimistic_neighbor_id))

    def unchoked(self, other_id):
        self.events.append(("unchoked", other_id))

    def choked(self, other_id):
        self.events.append(("choked", other_id))

    def receiving_have(self, other_id, piece_index):
        self.events.append(("have", other_id, piece_index))

    def receiving_interested(self, other_id):
        self.events.append(("interested", other_id))

    def receiving_not_interested(self, other_id):
        self.events.append(("not_interested", other_id))

    def downloading_piece(self, other_id, piece_index, number_of_pieces):
        self.events.append(("download", other_id, piece_index, number_of_pieces))

    def completion_of_download(self):
        self.events.append(("complete",))


class RecordingProtocolPeerNode(ProtocolPeerNode):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sent_messages: list[tuple[int, Message]] = []
        self.connections = {2: object(), 3: object()}

    def send_message(self, remote_id: int, msg: Message) -> None:
        self.sent_messages.append((remote_id, msg))


def make_node() -> RecordingProtocolPeerNode:
    peers = {
        1: PeerInfo(peer_id=1, host="127.0.0.1", port=6001, has_file=False),
        2: PeerInfo(peer_id=2, host="127.0.0.1", port=6002, has_file=False),
        3: PeerInfo(peer_id=3, host="127.0.0.1", port=6003, has_file=False),
    }
    common = CommonConfig(
        k=1,
        unchoke_interval=5,
        optimistic_unchoke_interval=15,
        filename="thefile",
        filesize=12,
        piecesize=4,
        num_pieces=3,
        last_piece_size=4,
    )
    return RecordingProtocolPeerNode(1, peers, common, DummyLogger())


def test_bitfield_drives_interest_and_request() -> None:
    node = make_node()
    node.on_connected(2)
    node.sent_messages.clear()

    node.on_message(2, Message(MsgType.BITFIELD, b"\xe0"))
    assert [(peer_id, msg.msg_type) for peer_id, msg in node.sent_messages] == [
        (2, MsgType.INTERESTED)
    ]

    node.on_message(2, Message(MsgType.UNCHOKE))
    assert node.sent_messages[-1][1].msg_type == MsgType.REQUEST
    assert parse_request_payload(node.sent_messages[-1][1].payload) == 0


def test_piece_reception_broadcasts_have_and_requests_next_piece() -> None:
    node = make_node()
    node.on_connected(2)
    node.on_connected(3)
    node.sent_messages.clear()

    node.on_message(2, Message(MsgType.BITFIELD, b"\xe0"))
    node.on_message(2, Message(MsgType.UNCHOKE))
    node.sent_messages.clear()

    node.on_message(2, Message(MsgType.PIECE, b"\x00\x00\x00\x00abcd"))

    sent_summary = [(peer_id, msg.msg_type) for peer_id, msg in node.sent_messages]
    assert (2, MsgType.HAVE) in sent_summary
    assert (3, MsgType.HAVE) in sent_summary
    assert sent_summary[-1] == (2, MsgType.REQUEST)
    assert parse_request_payload(node.sent_messages[-1][1].payload) == 1

    have_payloads = [msg.payload for peer_id, msg in node.sent_messages if msg.msg_type == MsgType.HAVE]
    assert all(parse_have_payload(payload) == 0 for payload in have_payloads)


def test_choke_clears_pending_request_and_unchoke_retries() -> None:
    node = make_node()
    node.on_connected(2)
    node.sent_messages.clear()

    node.on_message(2, Message(MsgType.BITFIELD, b"\xe0"))
    node.on_message(2, Message(MsgType.UNCHOKE))
    first_request = parse_request_payload(node.sent_messages[-1][1].payload)
    assert first_request == 0

    node.on_message(2, Message(MsgType.CHOKE))
    assert node.peer_states[2].requested_piece is None
    assert 0 not in node.pending_requests

    node.on_message(2, Message(MsgType.UNCHOKE))
    assert parse_request_payload(node.sent_messages[-1][1].payload) == 0


def run_all_tests() -> None:
    test_bitfield_drives_interest_and_request()
    test_piece_reception_broadcasts_have_and_requests_next_piece()
    test_choke_clears_pending_request_and_unchoke_retries()
    print("Message protocol flow tests passed")


if __name__ == "__main__":
    run_all_tests()
