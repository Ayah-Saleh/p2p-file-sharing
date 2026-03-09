import random
import threading
import bitfield

class ChokingHandler:
    def __init__(self, peer_id, logger, k, p_interval, m_interval):
        self.peer_id = peer_id
        self.logger = logger
        self.k = k
        self.p_interval = p_interval
        self.m_interval = m_interval
        self.bitfield = bitfield

        self.neighbors = {}
        self.optimistic_neighbor_id = None

    

        self.lock = threading.Lock()

    
    
    # when new peer connects, it gets added to the dictionary of neighbors
    # we use self.lock because there are mutliple threads running at once
    def add_neighbor(self, neighbor_id):
        with self.lock:
            self.neighbors[neighbor_id] = {"interested": False, "choked": True, "num_bytes_received": 0 }
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
        # the next run will be after p seconds
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
                return
            
            # if the file is complete, then we randomly choose k neighbors
            if self.complete_file():
                chosen_neighbor = random.sample(list_interested, min(len(list_interested), self.k)) 
            else:
                ranked_neighbors = sorted(list_interested, key=lambda x: self.neighbors[x]["num_bytes_received"], reverse=True)

                chosen_neighbor = self.pick_k_neighbors(ranked_neighbors)

            for id in self.neighbors:
                if id in chosen_neighbor:
                    self.neighbors[id]["choked"] = False
                elif id != self.optimistic_neighbor_id:
                    # all other neighbors get choked that were previously not unchoked, unless optimistic neighbor
                    self.neighbors[id]["choked"] = True
            for id in self.neighbors:
                self.neighbors[id]["num_bytes_received"] = 0
            
            self.logger.change_preferred_neighbors(chosen_neighbor)
    
    def complete_file(self):
        return self.bitfield.is_complete()
    
    def pick_k_neighbors(self, ranked_neighbors):
        # if the number of neighbors is less than k, then we return all neighbors
        if len(ranked_neighbors) <= self.k:
            return ranked_neighbors
        
        # the top k neigbors
        k_neigh = ranked_neighbors[:self.k]

        # the num of bytes for the boundary peer
        last_peer_byte = self.neighbors[k_neigh[-1]]["num_bytes_received"]
        # calc the neighbors that will be in the list bc bytes received are greater than last peer

        greater_than_last_peer =[]
        for id in k_neigh:
            if self.neighbors[id]["num_bytes_received"] > last_peer_byte:
                greater_than_last_peer.append(id)

        # neighbors that are tied for the same btw bytes received
        tie =[]
        for id in ranked_neighbors:
            if self.neighbors[id]["num_bytes_received"] == last_peer_byte:
                tie.append(id)
       
        # check how many spots are left for a tied peers
        space = self.k - len(greater_than_last_peer)

        tie_break = random.sample(tie, min(space, len(tie)))

        final_list = greater_than_last_peer + tie_break

        return final_list
        



    def select_opt_unchoked_neighbors(self):
        #Every m seconds, peer A reselects an optimistically unchoked 
        # neighbor randomly among neighbors that are choked at that moment
        #but areinterested in its data.
        with self.lock:
            choked_interested = []
            for neighbor_id in self.neighbors.keys():
                if self.neighbors[neighbor_id]["interested"] and self.neighbors[neighbor_id]["choked"]:
                    choked_interested.append(neighbor_id)
            if len(choked_interested) == 0:
                return
            
            self.optimistic_neighbor_id = random.choice(choked_interested)
            self.neighbors[self.optimistic_neighbor_id]["choked"] = False
            self.logger.change_optimistic_unchoked_neighbors(self.optimistic_neighbor_id)

    def stop(self):
        # does the ptimer exist, only then cancel
        if hasattr(self, 'ptimer'):
            self.ptimer.cancel()
        
        if hasattr(self, 'mtimer'):
            self.mtimer.cancel()






            
        