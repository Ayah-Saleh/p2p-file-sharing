import socket
import threading
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from protocol.framing import recv_exact, recv_message, send_message
from protocol.handshake import HANDSHAKE_LEN, build_handshake, parse_handshake
from protocol.messages import Message, MsgType


def server_main(port_holder: list[int]) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_sock:
        server_sock.bind(("127.0.0.1", 0))
        server_sock.listen(1)
        port_holder.append(server_sock.getsockname()[1])

        conn, _ = server_sock.accept()
        with conn:
            client_handshake = recv_exact(conn, HANDSHAKE_LEN)
            print("server parsed client handshake peer_id:", parse_handshake(client_handshake))

            conn.sendall(build_handshake(2002))

            msg = recv_message(conn)
            print("server received msg_type:", msg.msg_type.name)


def main() -> None:
    port_holder: list[int] = []
    server_thread = threading.Thread(target=server_main, args=(port_holder,), daemon=True)
    server_thread.start()

    while not port_holder:
        pass

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_sock:
        client_sock.connect(("127.0.0.1", port_holder[0]))

        client_sock.sendall(build_handshake(1001))
        server_handshake = recv_exact(client_sock, HANDSHAKE_LEN)
        print("client parsed server handshake peer_id:", parse_handshake(server_handshake))

        send_message(client_sock, Message(msg_type=MsgType.INTERESTED))

    server_thread.join(timeout=1.0)


if __name__ == "__main__":
    main()
