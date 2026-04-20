# peerProcess.py
# this is the entry point script that runs one peer in the network
import sys
import time
from logger import PeerLogger
from .config import read_common_cfg, read_peerinfo_cfg
from .protocol_node import ProtocolPeerNode

def main():
    # check that the user gave us a peer id as an argument
    if len(sys.argv) != 2:
        print("Usage: python peerProcess.py <peer_id>")
        sys.exit(1)

    # parse the peer id from the command line
    peer_id = int(sys.argv[1])

    # read the configuration files (spec section about config parsing)
    common = read_common_cfg("Common.cfg")
    peers = read_peerinfo_cfg("PeerInfo.cfg")

    # make sure our peer id is in the list of peers
    if peer_id not in peers:
        raise ValueError(f"Peer {peer_id} not found in PeerInfo.cfg")

    # get our own peer info
    me = peers[peer_id]

    # print out the loaded config for debugging
    print(f"[{peer_id}] Common.cfg:")
    print(f"  k={common.k}, unchoke={common.unchoke_interval}s, opt={common.optimistic_unchoke_interval}s")
    print(f"  file={common.filename}, size={common.filesize}, piece={common.piecesize}")
    print(f"  num_pieces={common.num_pieces}, last_piece_size={common.last_piece_size}")
    print(f"[{peer_id}] I am {me.host}:{me.port}, has_file={me.has_file}")

    # create the network node and start it (sets up server and connects to smaller peers)
    logger = PeerLogger(peer_id)
    node = ProtocolPeerNode(peer_id, peers, common, logger)
    node.start()

    try:
        # keep the peer running and print status periodically
        while True:
            time.sleep(2)
            # show which peers we're connected to
            print(f"[{peer_id}] Connected peers: {node.get_connected_peer_ids()}")
            if node.is_network_complete():
                print(f"[{peer_id}] All peers completed. Shutting down...")
                node.shutdown()
                break
    except KeyboardInterrupt:
        # shutdown when user presses Ctrl+C
        print(f"[{peer_id}] Shutting down...")
        node.shutdown()

if __name__ == "__main__":
    main()
