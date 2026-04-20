# config.py
from dataclasses import dataclass

# this stores the shared config params that all peers need to know about
# (how many neighbors, choking intervals, file info, etc)
@dataclass(frozen=True)
class CommonConfig:
    k: int  # how many neighbors we unchoke at the same time
    unchoke_interval: int  # how often (seconds) we pick new preferred neighbors
    optimistic_unchoke_interval: int  # how often (seconds) we pick a random neighbor
    filename: str  # name of the file we're sharing
    filesize: int  # total size in bytes
    piecesize: int  # how big each piece is (ex: 16kb)
    num_pieces: int  # calculated: total pieces = ceil(filesize / piecesize)
    last_piece_size: int  # the last piece might be smaller than piecesize

# this stores info about each peer - their id, address, port, whether they have the whole file
@dataclass(frozen=True)
class PeerInfo:
    peer_id: int  # unique id for this peer
    host: str  # ip address
    port: int  # port number
    has_file: bool  # true if they already have the complete file

def read_common_cfg(path: str = "Common.cfg") -> CommonConfig:
    # read the Common.cfg file and extract the values we need
    vals = {}
    with open(path, "r") as f:
        for line in f:
            # skip empty lines
            if not line.strip():
                continue
            # each line has key and value separated by space
            key, value = line.split()
            vals[key] = value

    # grab all the values from the config dict
    k = int(vals["NumberOfPreferredNeighbors"])
    unchoke = int(vals["UnchokingInterval"])
    opt = int(vals["OptimisticUnchokingInterval"])
    filename = vals["FileName"]
    filesize = int(vals["FileSize"])
    piecesize = int(vals["PieceSize"])

    # calculate how many pieces we need - round up if not evenly divisible
    num_pieces = (filesize + piecesize - 1) // piecesize
    # the last piece is whatever's left over after dividing into equal-sized pieces
    last_piece_size = filesize - (piecesize * (num_pieces - 1))

    return CommonConfig(
        k=k,
        unchoke_interval=unchoke,
        optimistic_unchoke_interval=opt,
        filename=filename,
        filesize=filesize,
        piecesize=piecesize,
        num_pieces=num_pieces,
        last_piece_size=last_piece_size,
    )

def read_peerinfo_cfg(path: str = "PeerInfo.cfg") -> dict[int, PeerInfo]:
    # read all the peers from PeerInfo.cfg
    peers: dict[int, PeerInfo] = {}
    with open(path, "r") as f:
        for line in f:
            # skip empty lines
            if not line.strip():
                continue
            # each line has: peer_id, host, port, has_file (1 or 0)
            pid, host, port, has_file = line.split()
            pid_i = int(pid)
            # convert the has_file string "1" or "0" to True/False
            peers[pid_i] = PeerInfo(
                peer_id=pid_i,
                host=host,
                port=int(port),
                has_file=(has_file == "1"),
            )
    # return the peers sorted by their peer_id so they're in a consistent order
    return dict(sorted(peers.items()))