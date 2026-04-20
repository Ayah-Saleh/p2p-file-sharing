import random
import threading


class ChokingHandler:
    def __init__(self, peer_id, logger, k, p_interval, m_interval, bitfield_state, state_change_callback):
        self.peer_id = peer_id
        self.logger = logger
        self.k = k
        self.p_interval = p_interval
        self.m_interval = m_interval
        self.bitfield = bitfield_state
        self.state_change_callback = state_change_callback
        self.neighbors = {}
        self.optimistic_neighbor_id = None
        self.lock = threading.Lock()

    # when new peer connects, it gets added to the dictionary of neighbors
    # we use self.lock because there are multiple threads running at once
    def add_neighbor(self, neighbor_id):
        with self.lock:
            self.neighbors[neighbor_id] = {"interested": False, "choked": True, "num_bytes_received": 0}

    def remove_neighbor(self, neighbor_id):
        with self.lock:
            self.neighbors.pop(neighbor_id, None)
            if self.optimistic_neighbor_id == neighbor_id:
                self.optimistic_neighbor_id = None

    def set_interested(self, neighbor_id, is_interested):
        with self.lock:
            if neighbor_id in self.neighbors:
                self.neighbors[neighbor_id]["interested"] = is_interested

    def record_bytes_received(self, neighbor_id, num_bytes_received):
        with self.lock:
            if neighbor_id in self.neighbors:
                self.neighbors[neighbor_id]["num_bytes_received"] += num_bytes_received

    def start_timer(self):
        self.run_preferred_neighbors()
        self.run_opt_unchoked_neighbors()

    def run_preferred_neighbors(self):
        self.select_preferred_neighbors()
        # the next run will be after p seconds
        self.ptimer = threading.Timer(self.p_interval, self.run_preferred_neighbors)
        self.ptimer.daemon = True
        self.ptimer.start()

    def run_opt_unchoked_neighbors(self):
        self.select_opt_unchoked_neighbors()
        # the next run will be after m seconds
        self.mtimer = threading.Timer(self.m_interval, self.run_opt_unchoked_neighbors)
        self.mtimer.daemon = True
        self.mtimer.start()

    def select_preferred_neighbors(self):
        with self.lock:
            list_interested = []
            # preferred neighbors are neighbors that are interested
            # creating list of interested neighbors
            for neighbor_id in self.neighbors.keys():
                if self.neighbors[neighbor_id]["interested"]:
                    list_interested.append(neighbor_id)

            if len(list_interested) == 0:
                for neighbor_id in self.neighbors:
                    if neighbor_id != self.optimistic_neighbor_id:
                        self.neighbors[neighbor_id]["choked"] = True
                self._notify_state_change()
                return

            # if the file is complete, then we randomly choose k neighbors
            if self.complete_file():
                chosen_neighbor = random.sample(list_interested, min(len(list_interested), self.k))
            else:
                ranked_neighbors = sorted(
                    list_interested,
                    key=lambda neighbor_id: self.neighbors[neighbor_id]["num_bytes_received"],
                    reverse=True,
                )
                chosen_neighbor = self.pick_k_neighbors(ranked_neighbors)

            for neighbor_id in self.neighbors:
                if neighbor_id in chosen_neighbor:
                    self.neighbors[neighbor_id]["choked"] = False
                elif neighbor_id != self.optimistic_neighbor_id:
                    # all other neighbors get choked, unless this is the optimistic neighbor
                    self.neighbors[neighbor_id]["choked"] = True

            for neighbor_id in self.neighbors:
                self.neighbors[neighbor_id]["num_bytes_received"] = 0

            self.logger.change_preferred_neighbors(chosen_neighbor)
            self._notify_state_change()

    def complete_file(self):
        return self.bitfield.is_complete()

    def pick_k_neighbors(self, ranked_neighbors):
        # if the number of neighbors is less than k, then we return all neighbors
        if len(ranked_neighbors) <= self.k:
            return ranked_neighbors

        # the top k neighbors
        k_neighbors = ranked_neighbors[:self.k]

        # the num of bytes for the boundary peer
        last_peer_byte = self.neighbors[k_neighbors[-1]]["num_bytes_received"]

        # calc the neighbors that will be in the list because bytes received are greater than last peer
        greater_than_last_peer = []
        for neighbor_id in k_neighbors:
            if self.neighbors[neighbor_id]["num_bytes_received"] > last_peer_byte:
                greater_than_last_peer.append(neighbor_id)

        # neighbors that are tied for the same bytes received
        tie = []
        for neighbor_id in ranked_neighbors:
            if self.neighbors[neighbor_id]["num_bytes_received"] == last_peer_byte:
                tie.append(neighbor_id)

        # check how many spots are left for the tied peers
        space = self.k - len(greater_than_last_peer)
        tie_break = random.sample(tie, min(space, len(tie)))
        final_list = greater_than_last_peer + tie_break
        return final_list

    def select_opt_unchoked_neighbors(self):
        # every m seconds, peer A reselects an optimistically unchoked
        # neighbor randomly among neighbors that are choked at that moment
        # but are interested in its data
        with self.lock:
            choked_interested = []
            for neighbor_id in self.neighbors.keys():
                if self.neighbors[neighbor_id]["interested"] and self.neighbors[neighbor_id]["choked"]:
                    choked_interested.append(neighbor_id)

            if len(choked_interested) == 0:
                return

            previous_neighbor = self.optimistic_neighbor_id
            self.optimistic_neighbor_id = random.choice(choked_interested)

            if previous_neighbor in self.neighbors and previous_neighbor != self.optimistic_neighbor_id:
                self.neighbors[previous_neighbor]["choked"] = True

            self.neighbors[self.optimistic_neighbor_id]["choked"] = False
            self.logger.change_optimistic_unchoked_neighbors(self.optimistic_neighbor_id)
            self._notify_state_change()

    def stop(self):
        # does the ptimer exist, only then cancel
        if hasattr(self, "ptimer"):
            self.ptimer.cancel()

        if hasattr(self, "mtimer"):
            self.mtimer.cancel()

    def _notify_state_change(self):
        snapshot = {}
        for neighbor_id, state in self.neighbors.items():
            snapshot[neighbor_id] = state["choked"]
        self.state_change_callback(snapshot)
