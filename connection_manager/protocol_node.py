from dataclasses import dataclass
import os
import random
import threading
from typing import Optional

from bitfield import Bitfield
from choking_handler import ChokingHandler
from logger import PeerLogger
from protocol.messages import (
    Message,
    MsgType,
    build_have_payload,
    build_piece_payload,
    build_request_payload,
    parse_have_payload,
    parse_piece_payload,
    parse_request_payload,
)

from .config import CommonConfig, PeerInfo
from .network import PeerNode


class FileStorage:
    # this class deals with reading and writing the actual file pieces to disk
    # each peer gets its own folder so files do not overwrite each other
    def __init__(self, peer_id: int, common: CommonConfig, has_file: bool):
        self.common = common
        self.peer_dir = f"peer_{peer_id}"
        self.file_path = os.path.join(self.peer_dir, common.filename)

        # create the peer folder if it does not exist
        os.makedirs(self.peer_dir, exist_ok=True)

        if has_file:
            # seeded peers are expected to already have the full file in place
            if not os.path.exists(self.file_path):
                raise FileNotFoundError(
                    f"peer {peer_id} is supposed to have {common.filename} "
                    f"but {self.file_path} doesn't exist"
                )
        else:
            # peers without the full file get a pre-sized placeholder file
            if not os.path.exists(self.file_path):
                with open(self.file_path, "wb") as file_obj:
                    file_obj.seek(common.filesize - 1)
                    file_obj.write(b"\x00")

    def piece_size(self, piece_index: int) -> int:
        # the last piece can be smaller than the normal piece size
        if piece_index == self.common.num_pieces - 1:
            return self.common.last_piece_size
        return self.common.piecesize

    def read_piece(self, piece_index: int) -> bytes:
        offset = piece_index * self.common.piecesize
        size = self.piece_size(piece_index)
        with open(self.file_path, "rb") as file_obj:
            file_obj.seek(offset)
            data = file_obj.read(size)
        if len(data) != size:
            raise IOError(f"could only read {len(data)} bytes for piece {piece_index}, expected {size}")
        return data

    def write_piece(self, piece_index: int, data: bytes) -> None:
        offset = piece_index * self.common.piecesize
        expected = self.piece_size(piece_index)
        if len(data) != expected:
            raise ValueError(
                f"piece {piece_index} data is {len(data)} bytes but expected {expected}"
            )
        with open(self.file_path, "r+b") as file_obj:
            file_obj.seek(offset)
            file_obj.write(data)


@dataclass
class PeerMessageState:
    # this tracks our protocol state with one specific neighbor
    peer_bitfield: Bitfield
    peer_choking_me: bool = True
    am_choking_peer: bool = True
    am_interested: bool = False
    peer_interested: bool = False
    requested_piece: Optional[int] = None


class ProtocolPeerNode(PeerNode):
    # this extends PeerNode with the actual protocol logic
    def __init__(
        self,
        self_id: int,
        peers: dict[int, PeerInfo],
        common: CommonConfig,
        logger: PeerLogger,
    ):
        super().__init__(self_id, peers)
        self.common = common
        self.logger = logger
        self.local_bitfield = Bitfield(common.num_pieces, peers[self_id].has_file)
        self.file_storage = FileStorage(self_id, common, peers[self_id].has_file)
        self.pending_requests: set[int] = set()
        self.peer_states: dict[int, PeerMessageState] = {}
        self.state_lock = threading.Lock()
        self.all_complete_event = threading.Event()
        self.choking_handler = ChokingHandler(
            self_id,
            logger,
            common.k,
            common.unchoke_interval,
            common.optimistic_unchoke_interval,
            bitfield_state=self.local_bitfield,
            state_change_callback=self._on_choke_state_change,
        )

    def start(self) -> None:
        super().start()
        self.choking_handler.start_timer()

    def shutdown(self) -> None:
        self.choking_handler.stop()
        super().shutdown()

    def on_connected(self, remote_id: int) -> None:
        # create protocol state as soon as the handshake is done
        with self.state_lock:
            self.peer_states.setdefault(
                remote_id,
                PeerMessageState(peer_bitfield=Bitfield(self.common.num_pieces)),
            )

        self.choking_handler.add_neighbor(remote_id)

        # send our current bitfield right after the connection is ready
        if not self.local_bitfield.is_empty():
            self.send_message(remote_id, Message(MsgType.BITFIELD, self.local_bitfield.to_bytes()))

        self._update_completion_state()

    def on_message(self, remote_id: int, msg: Message) -> None:
        handlers = {
            MsgType.CHOKE: self._handle_choke,
            MsgType.UNCHOKE: self._handle_unchoke,
            MsgType.INTERESTED: self._handle_interested,
            MsgType.NOT_INTERESTED: self._handle_not_interested,
            MsgType.HAVE: self._handle_have,
            MsgType.BITFIELD: self._handle_bitfield,
            MsgType.REQUEST: self._handle_request,
            MsgType.PIECE: self._handle_piece,
        }
        handlers[msg.msg_type](remote_id, msg)

    def on_disconnected(self, remote_id: int) -> None:
        # free any outstanding request we had assigned to this peer
        with self.state_lock:
            state = self.peer_states.pop(remote_id, None)
            if state is not None and state.requested_piece is not None:
                self.pending_requests.discard(state.requested_piece)

        self.choking_handler.remove_neighbor(remote_id)
        self._update_completion_state()

    def _handle_choke(self, remote_id: int, msg: Message) -> None:
        with self.state_lock:
            state = self._get_state(remote_id)
            state.peer_choking_me = True
            if state.requested_piece is not None:
                self.pending_requests.discard(state.requested_piece)
                state.requested_piece = None
        self.logger.choked(remote_id)

    def _handle_unchoke(self, remote_id: int, msg: Message) -> None:
        with self.state_lock:
            self._get_state(remote_id).peer_choking_me = False
        self.logger.unchoked(remote_id)
        self._request_next_piece(remote_id)

    def _handle_interested(self, remote_id: int, msg: Message) -> None:
        with self.state_lock:
            self._get_state(remote_id).peer_interested = True
        self.choking_handler.set_interested(remote_id, True)
        self.logger.receiving_interested(remote_id)

    def _handle_not_interested(self, remote_id: int, msg: Message) -> None:
        with self.state_lock:
            self._get_state(remote_id).peer_interested = False
        self.choking_handler.set_interested(remote_id, False)
        self.logger.receiving_not_interested(remote_id)

    def _handle_have(self, remote_id: int, msg: Message) -> None:
        piece_index = parse_have_payload(msg.payload)
        self.logger.receiving_have(remote_id, piece_index)

        with self.state_lock:
            self._get_state(remote_id).peer_bitfield.set_piece(piece_index)

        self._sync_interest(remote_id)
        self._request_next_piece(remote_id)
        self._update_completion_state()

    def _handle_bitfield(self, remote_id: int, msg: Message) -> None:
        peer_bitfield = Bitfield.from_bytes(msg.payload, self.common.num_pieces)

        with self.state_lock:
            self._get_state(remote_id).peer_bitfield = peer_bitfield

        self._sync_interest(remote_id)
        self._request_next_piece(remote_id)
        self._update_completion_state()

    def _handle_request(self, remote_id: int, msg: Message) -> None:
        piece_index = parse_request_payload(msg.payload)

        with self.state_lock:
            if self._get_state(remote_id).am_choking_peer:
                return

        # ignore requests for pieces we do not currently have
        if not self.local_bitfield.has_piece(piece_index):
            return

        piece_data = self.file_storage.read_piece(piece_index)
        payload = build_piece_payload(piece_index, piece_data)
        self.send_message(remote_id, Message(MsgType.PIECE, payload))

    def _handle_piece(self, remote_id: int, msg: Message) -> None:
        piece_index, piece_data = parse_piece_payload(msg.payload)

        with self.state_lock:
            state = self._get_state(remote_id)
            if state.requested_piece == piece_index:
                state.requested_piece = None
            self.pending_requests.discard(piece_index)

            already_had_piece = self.local_bitfield.has_piece(piece_index)
            if not already_had_piece:
                self.file_storage.write_piece(piece_index, piece_data)
                self.local_bitfield.set_piece(piece_index)
                piece_count = self.local_bitfield.count()
            else:
                piece_count = self.local_bitfield.count()

        if piece_data and not already_had_piece:
            self.choking_handler.record_bytes_received(remote_id, len(piece_data))
            self.logger.downloading_piece(remote_id, piece_index, piece_count)

            if self.local_bitfield.is_complete():
                self.logger.completion_of_download()

            self._broadcast_have(piece_index)

        self._refresh_interest_for_all_peers()
        self._request_next_piece(remote_id)
        self._update_completion_state()

    def _sync_interest(self, remote_id: int) -> None:
        with self.state_lock:
            state = self._get_state(remote_id)
            interesting = bool(self.local_bitfield.interesting_pieces(state.peer_bitfield))

            if interesting == state.am_interested:
                return

            state.am_interested = interesting

        if interesting:
            self.send_message(remote_id, Message(MsgType.INTERESTED))
        else:
            self.send_message(remote_id, Message(MsgType.NOT_INTERESTED))

    def _request_next_piece(self, remote_id: int) -> None:
        with self.state_lock:
            state = self._get_state(remote_id)

            # only keep one outstanding request per peer and stop when choked
            if state.peer_choking_me or state.requested_piece is not None:
                return

            next_piece = self._choose_piece_to_request(state.peer_bitfield)
            if next_piece is None:
                return

            state.requested_piece = next_piece
            self.pending_requests.add(next_piece)

        self.send_message(remote_id, Message(MsgType.REQUEST, build_request_payload(next_piece)))

    def _choose_piece_to_request(self, peer_bitfield: Bitfield) -> Optional[int]:
        # skip pieces that are already being requested from another neighbor
        candidates = []
        for piece_index in self.local_bitfield.interesting_pieces(peer_bitfield):
            if piece_index not in self.pending_requests:
                candidates.append(piece_index)

        if not candidates:
            return None

        return random.choice(candidates)

    def _broadcast_have(self, piece_index: int) -> None:
        with self.conn_lock:
            remote_ids = list(self.connections.keys())

        payload = build_have_payload(piece_index)
        for remote_id in remote_ids:
            self.send_message(remote_id, Message(MsgType.HAVE, payload))

    def _refresh_interest_for_all_peers(self) -> None:
        with self.state_lock:
            remote_ids = list(self.peer_states.keys())

        for remote_id in remote_ids:
            self._sync_interest(remote_id)

    def _get_state(self, remote_id: int) -> PeerMessageState:
        return self.peer_states.setdefault(
            remote_id,
            PeerMessageState(peer_bitfield=Bitfield(self.common.num_pieces)),
        )

    def _on_choke_state_change(self, choke_state: dict[int, bool]) -> None:
        messages_to_send: list[tuple[int, MsgType]] = []

        with self.state_lock:
            for remote_id, is_choked in choke_state.items():
                state = self._get_state(remote_id)
                if state.am_choking_peer == is_choked:
                    continue
                state.am_choking_peer = is_choked
                if is_choked:
                    messages_to_send.append((remote_id, MsgType.CHOKE))
                else:
                    messages_to_send.append((remote_id, MsgType.UNCHOKE))

        for remote_id, msg_type in messages_to_send:
            self.send_message(remote_id, Message(msg_type))

    def is_network_complete(self) -> bool:
        return self.all_complete_event.is_set()

    def _update_completion_state(self) -> None:
        with self.state_lock:
            if not self.local_bitfield.is_complete():
                return

            expected_peer_count = len(self.peers) - 1
            if len(self.peer_states) < expected_peer_count:
                return

            for peer_id in self.peers:
                if peer_id == self.self_id:
                    continue
                state = self.peer_states.get(peer_id)
                if state is None or not state.peer_bitfield.is_complete():
                    return

        self.all_complete_event.set()
