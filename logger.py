import logging


class PeerLogger:
    def __init__(self, peer_id):
        self.peer_id = peer_id
        self.logger = logging.getLogger(f"peer_{peer_id}")
        self.logger.setLevel(logging.INFO)
        
        log_filename = f"log_peer_{peer_id}.log"
        file_handler = logging.FileHandler(log_filename, mode='w')

        # asctime is the timestamp, and treats it as a string, message is the message that will be passed into log function
        formatter  = logging.Formatter('[%(asctime)s]: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        
        file_handler.setFormatter(formatter)

        self.logger.addHandler(file_handler)

    def log_tcp_connection_to_peer(self, other_id):
        self.logger.info(f"Peer {self.peer_id} makes a connection to Peer {other_id}.")
    def log_tcp_connection_from_peer(self, other_id):
        self.logger.info(f"Peer {self.peer_id} is connected from Peer {other_id}.")

    def change_preferred_neighbors(self, preferred_neighbors):
        list_id = ",".join(map(str,preferred_neighbors))
        self.logger.info(f"Peer {self.peer_id} has the preferred neighbors {list_id}.")
    def change_optimistic_unchoked_neighbors(self, optimistic_neighbor_id):
        self.logger.info(f"Peer {self.peer_id} has the optimistically unchoked neighbor {optimistic_neighbor_id}.")
    def unchoked(self, other_id):
        self.logger.info(f"Peer {self.peer_id} is unchoked by Peer {other_id}.")
    def choked(self, other_id):
        self.logger.info(f"Peer {self.peer_id} is choked by Peer {other_id}.")
    def receiving_have(self, other_id, piece_index):
        self.logger.info(f"Peer {self.peer_id} received the 'have' message from Peer {other_id} for the piece {piece_index}.")
    def receiving_interested(self, other_id):
        self.logger.info(f"Peer {self.peer_id} received the 'interested' message from Peer {other_id}.")
    def receiving_not_interested(self, other_id):
        self.logger.info(f"Peer {self.peer_id} received the 'not interested' message from Peer {other_id}.")
    def downloading_piece(self, other_id, piece_index, number_of_pieces):
        self.logger.info(f"Peer {self.peer_id} has downloaded the piece {piece_index} from Peer {other_id}. Now the number of pieces it has is {number_of_pieces}.")
    def completion_of_download(self):
        self.logger.info(f"Peer {self.peer_id} has downloaded the complete file.")
