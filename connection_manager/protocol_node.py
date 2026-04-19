from dataclasses import dataclass
import threading
from typing import Optional
from choking_handler import ChokingHandler

from bitfield import Bitfield
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


@dataclass
class PeerMessageState:
    peer_bitfield: Bitfield
    peer_choking_me: bool = True
    am_interested: bool = False
    peer_interested: bool = False
    requested_piece: Optional[int] = None


class ProtocolPeerNode(PeerNode):
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
        self.pending_requests: set[int] = set()
        self.peer_states: dict[int, PeerMessageState] = {}
        self.state_lock = threading.Lock()

        self.choking_handler = ChokingHandler(peer_id=self_id,
                                              logger=logger,
                                              k=common.k,
                                              p_interval=common.unchoke_interval,
                                              m_interval=common.optimistic_unchoke_interval,
                                              bitfield=self.local_bitfield,
                                              protocol_node=self)

    def on_connected(self, remote_id: int) -> None:
        # create protocol state for the peer as soon as the handshake is done
        with self.state_lock:
            self.peer_states.setdefault(
                remote_id,
                PeerMessageState(peer_bitfield=Bitfield(self.common.num_pieces)),
            )

        # send our current piece availability right after the connection is ready
        if not self.local_bitfield.is_empty():
            self.send_message(remote_id, Message(MsgType.BITFIELD, self.local_bitfield.to_bytes()))

        # Keep the protocol moving while choking policy is still a placeholder.
        self.send_message(remote_id, Message(MsgType.UNCHOKE))

        self.choking_handler.add_neighbor(remote_id)
        self.choking_handler.start_timer()

    def on_message(self, remote_id: int, msg: Message) -> None:
        # route each incoming message to the matching protocol handler
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
        with self.state_lock:
            state = self.peer_states.pop(remote_id, None)
            if state is not None and state.requested_piece is not None:
                self.pending_requests.discard(state.requested_piece)

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
        self.logger.receiving_interested(remote_id)

    def _handle_not_interested(self, remote_id: int, msg: Message) -> None:
        with self.state_lock:
            self._get_state(remote_id).peer_interested = False
        self.logger.receiving_not_interested(remote_id)

    def _handle_have(self, remote_id: int, msg: Message) -> None:
        piece_index = parse_have_payload(msg.payload)
        self.logger.receiving_have(remote_id, piece_index)

        with self.state_lock:
            self._get_state(remote_id).peer_bitfield.set_piece(piece_index)

        self._sync_interest(remote_id)
        self._request_next_piece(remote_id)

    def _handle_bitfield(self, remote_id: int, msg: Message) -> None:
        peer_bitfield = Bitfield.from_bytes(msg.payload, self.common.num_pieces)

        with self.state_lock:
            self._get_state(remote_id).peer_bitfield = peer_bitfield

        self._sync_interest(remote_id)
        self._request_next_piece(remote_id)

    def _handle_request(self, remote_id: int, msg: Message) -> None:
        piece_index = parse_request_payload(msg.payload)

        # ignore requests for pieces we do not currently have
        if not self.local_bitfield.has_piece(piece_index):
            return

        # piece storage is still a placeholder, so respond with zeroed bytes of the right size
        piece_size = self._piece_size(piece_index)
        piece_data = b"\x00" * piece_size
        payload = build_piece_payload(piece_index, piece_data)
        self.send_message(remote_id, Message(MsgType.PIECE, payload))

    def _handle_piece(self, remote_id: int, msg: Message) -> None:
        piece_index, piece_data = parse_piece_payload(msg.payload)

        self.choking_handler.record_bytes_received(remote_id, len(piece_data))

        with self.state_lock:
            state = self._get_state(remote_id)
            if state.requested_piece == piece_index:
                state.requested_piece = None
            self.pending_requests.discard(piece_index)

            already_had_piece = self.local_bitfield.has_piece(piece_index)
            if not already_had_piece:
                self.local_bitfield.set_piece(piece_index)
                piece_count = self.local_bitfield.count()
            else:
                piece_count = self.local_bitfield.count()

        if piece_data and not already_had_piece:
            self.logger.downloading_piece(remote_id, piece_index, piece_count)

            if self.local_bitfield.is_complete():
                self.logger.completion_of_download()

            self._broadcast_have(piece_index)

        self._refresh_interest_for_all_peers()
        self._request_next_piece(remote_id)

    def _sync_interest(self, remote_id: int) -> None:
        with self.state_lock:
            state = self._get_state(remote_id)
            # compare what the neighbor has against what we still need
            interesting = bool(self.local_bitfield.interesting_pieces(state.peer_bitfield))

            if interesting == state.am_interested:
                return

            state.am_interested = interesting

        if interesting:
            self.send_message(remote_id, Message(MsgType.INTERESTED))
        else:
            self.send_message(remote_id, Message(MsgType.NOT_INTERESTED))
        
        self.choking_handler.set_interested(remote_id, interesting)

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
        for piece_index in self.local_bitfield.interesting_pieces(peer_bitfield):
            if piece_index not in self.pending_requests:
                return piece_index
        return None

    def _broadcast_have(self, piece_index: int) -> None:
        # let every connected peer know we finished one more piece
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

    def _piece_size(self, piece_index: int) -> int:
        # the last piece can be smaller than the standard piece size
        if piece_index == self.common.num_pieces - 1:
            return self.common.last_piece_size
        return self.common.piecesize

    def _get_state(self, remote_id: int) -> PeerMessageState:
        return self.peer_states.setdefault(
            remote_id,
            PeerMessageState(peer_bitfield=Bitfield(self.common.num_pieces)),
        )
