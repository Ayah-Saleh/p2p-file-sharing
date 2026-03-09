# network.py
# this handles all the tcp connection stuff and the handshake that happens
# when two peers first connect to each other
import socket
import struct
import threading
from typing import Dict, Optional, Tuple

# the spec says the handshake is 32 bytes total
# first 18 bytes are the string "P2PFILESHARINGPROJ"
HANDSHAKE_HEADER = b"P2PFILESHARINGPROJ"
# next 10 bytes are all zeros
HANDSHAKE_ZEROS = b"\x00" * 10
# so total is 18 + 10 + 4 = 32 bytes
HANDSHAKE_LEN = 32

def build_handshake(peer_id: int) -> bytes:
    # create the 32-byte handshake: header + zeros + peer_id as 4 bytes (big-endian)
    return HANDSHAKE_HEADER + HANDSHAKE_ZEROS + struct.pack("!I", peer_id)

def recv_exact(sock: socket.socket, n: int) -> bytes:
    # keep reading from socket until we get exactly n bytes
    # (sometimes recv returns less than we ask for, so we need to loop)
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        # if we get nothing, the connection closed
        if not chunk:
            raise ConnectionError("Socket closed while receiving")
        buf += chunk
    return buf

def parse_handshake(data: bytes) -> int:
    # verify the handshake is the right format and extract the peer id
    if len(data) != HANDSHAKE_LEN:
        raise ValueError(f"Handshake wrong length: {len(data)}")
    # first 18 bytes should be the header
    if data[:18] != HANDSHAKE_HEADER:
        raise ValueError("Bad handshake header")
    # next 10 bytes should be all zeros
    if data[18:28] != HANDSHAKE_ZEROS:
        raise ValueError("Bad handshake zero bits")
    # last 4 bytes are the peer id (big-endian unsigned int)
    return struct.unpack("!I", data[28:32])[0]


class PeerNode:
    # this class manages all the network connections for one peer
    # it acts as both a server (accepting connections from other peers)
    # and a client (connecting to other peers)
    def __init__(self, self_id: int, peers: Dict[int, "PeerInfo"]):
        self.self_id = self_id  # my own peer id
        self.peers = peers  # dict of all peers from PeerInfo.cfg
        self.me = peers[self_id]  # my own peer info (host, port, etc)

        self.server_sock: Optional[socket.socket] = None  # the server socket that listens
        self.accept_thread: Optional[threading.Thread] = None  # thread that accepts connections

        # lock to protect the connections dict so multiple threads don't mess it up
        self.conn_lock = threading.Lock()
        # dict mapping peer_id -> socket for all active connections
        self.connections: Dict[int, socket.socket] = {}
        # dict mapping peer_id -> thread for each peer's reader thread
        self.reader_threads: Dict[int, threading.Thread] = {}

        # flag to tell all threads to stop
        self.shutdown_event = threading.Event()

    # -------- public API --------
    def start(self) -> None:
        # start the server (listen for incoming connections) and connect to smaller peers
        self._start_server()
        self._connect_to_smaller_peers()

    def shutdown(self) -> None:
        # close all sockets and stop all threads gracefully
        self.shutdown_event.set()

        # close the server socket
        if self.server_sock:
            try:
                self.server_sock.close()
            except OSError:
                pass

        # close all peer connections
        with self.conn_lock:
            socks = list(self.connections.values())
            self.connections.clear()

        # shut down each socket
        for s in socks:
            try:
                s.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            try:
                s.close()
            except OSError:
                pass

    def get_connected_peer_ids(self) -> list[int]:
        # return a sorted list of all peer ids we're currently connected to
        with self.conn_lock:
            return sorted(self.connections.keys())

    # -------- internal server/client behaviors --------
    def _start_server(self) -> None:
        # create a server socket that listens for incoming connections
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # allow reusing the address if the socket is closed and reopened quickly
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # TCP_NODELAY disables nagle's algorithm so messages go out immediately
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        # bind to our host and port from PeerInfo.cfg
        s.bind((self.me.host, self.me.port))
        # listen for incoming connections
        s.listen()

        self.server_sock = s
        # start a thread to accept incoming connections
        self.accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
        self.accept_thread.start()

        print(f"[{self.self_id}] Listening on {self.me.host}:{self.me.port}")

    def _accept_loop(self) -> None:
        # continuously accept incoming connections from other peers
        assert self.server_sock is not None

        while not self.shutdown_event.is_set():
            try:
                # accept a new connection
                sock, addr = self.server_sock.accept()
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except OSError:
                break  # server socket got closed

            try:
                # the spec says: incoming connection, they send handshake first
                their = recv_exact(sock, HANDSHAKE_LEN)
                remote_id = parse_handshake(their)

                # check if this peer is in our PeerInfo.cfg
                if remote_id not in self.peers:
                    raise ValueError(f"Unknown peer id {remote_id}")

                # send our handshake back to them
                sock.sendall(build_handshake(self.self_id))
                # add this connection to our active connections
                self._register_connection(remote_id, sock, incoming=True)
            except Exception as e:
                print(f"[{self.self_id}] Reject incoming connection: {e}")
                try:
                    sock.close()
                except OSError:
                    pass

    def _connect_to_smaller_peers(self) -> None:
        # connect to all peers with smaller peer_ids than us
        # (this way each pair only makes one connection: the higher id connects to the lower)
        for pid in sorted(self.peers.keys()):
            if pid < self.self_id:
                self._connect_to_peer(pid)

    def _connect_to_peer(self, peer_id: int) -> None:
        # initiate an outgoing connection to a specific peer
        peer = self.peers[peer_id]
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        print(f"[{self.self_id}] Connecting to {peer_id} at {peer.host}:{peer.port} ...")
        sock.connect((peer.host, peer.port))

        # spec says: outgoing connection, we send handshake first
        sock.sendall(build_handshake(self.self_id))
        # then wait for their handshake back
        their = recv_exact(sock, HANDSHAKE_LEN)
        remote_id = parse_handshake(their)

        # make sure we got a handshake from the right peer
        if remote_id != peer_id:
            sock.close()
            raise ValueError(f"Connected to wrong peer: expected {peer_id}, got {remote_id}")

        # add this connection to our active connections
        self._register_connection(remote_id, sock, incoming=False)

    def _register_connection(self, remote_id: int, sock: socket.socket, incoming: bool) -> None:
        # add a new connection to our list and start a reader thread for it
        with self.conn_lock:
            # if we already have a connection to this peer, ignore the duplicate
            if remote_id in self.connections:
                try:
                    sock.close()
                except OSError:
                    pass
                return
            self.connections[remote_id] = sock

        # print whether this was an incoming or outgoing connection
        direction = "IN " if incoming else "OUT"
        print(f"[{self.self_id}] {direction} connected with peer {remote_id}")

        # call the on_connected hook so the protocol layer knows we're ready
        self.on_connected(remote_id)

        # start a reader thread to read data from this peer's socket
        t = threading.Thread(target=self._reader_loop, args=(remote_id, sock), daemon=True)
        self.reader_threads[remote_id] = t
        t.start()

    def _reader_loop(self, remote_id: int, sock: socket.socket) -> None:
        # continuously read data from a peer's socket
        # (this is where we'll parse messages later)
        try:
            while not self.shutdown_event.is_set():
                # try to read up to 4kb of data from the socket
                data = sock.recv(4096)
                if not data:
                    # if we get no data, the connection closed
                    break
                # hand the raw data off to the protocol layer via the hook
                self.on_raw_data(remote_id, data)
        except OSError:
            # socket was closed or errored
            pass
        finally:
            # clean up: remove from connections dict and close the socket
            with self.conn_lock:
                self.connections.pop(remote_id, None)
            try:
                sock.close()
            except OSError:
                pass
            print(f"[{self.self_id}] Disconnected from peer {remote_id}")
            # call the on_disconnected hook
            self.on_disconnected(remote_id)

    # -------- hooks for the protocol layer to implement --------
    def on_connected(self, remote_id: int) -> None:
        # called when handshake is done and connection is active
        # protocol layer should override this to send bitfield
        pass

    def on_raw_data(self, remote_id: int, data: bytes) -> None:
        # called when we receive raw bytes from a peer
        # protocol layer should override this to parse messages
        return

    def on_disconnected(self, remote_id: int) -> None:
        # called when the connection closes
        # protocol layer can override this to clean up
        pass


# we import PeerInfo at the end to avoid circular imports
from .config import PeerInfo  # noqa: E402
