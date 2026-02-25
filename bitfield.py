"""
From spec:
‘bitfield’ messages is only sent as the first message right after handshaking is done when
a connection is established. ‘bitfield’ messages have a bitfield as its payload. Each bit in
the bitfield payload represents whether the peer has the corresponding piece or not. The
first byte of the bitfield corresponds to piece indices 0 – 7 from high bit to low bit,
respectively. The next one corresponds to piece indices 8 – 15, etc. Spare bits at the end
are set to zero. Peers that don’t have anything yet may skip a ‘bitfield’ message.
"""

class Bitfield:
    def __init__(self, num_pieces: int, has_file: bool = False):
        self.num_pieces = num_pieces
        if has_file: #if has file then fill bits with 1 (peer has full file)
            self.bits = [1] * num_pieces
        else: #otherwise you dont have file so fill with 0
            self.bits = [0] * num_pieces

    def set_piece(self, index: int): #mark piece as downloaded (set to 1)
        self._validate(index)
        self.bits[index] = 1 

    def has_piece(self, index: int): #true if peice at index is present
        self._validate(index)
        return self.bits[index] == 1

    def is_complete(self): #returns true if all peices are downloaded
        for bit in self.bits:
            if bit == 0:
                return False  #missing peice
        return True

    def is_empty(self): #returns true if no peices are downloaded
        for bit in self.bits:
            if bit == 1:
                return False  #present peice
        return True

    def count(self): #amount of peices downloaded
        total = 0
        for bit in self.bits:
            if bit == 1:
                total += 1
        return total

    def interesting_pieces(self, other_bitfield): #this returns a list in increasing order, spec wants random so do that elsewhere
        result = []
        for i in range(self.num_pieces):
            if other_bitfield.bits[i] == 1 and self.bits[i] == 0: #check if peer has peice we dont
                result.append(i) #if they do, add it to list of peices that we are interested in
        return result

    def to_bytes(self):
        """
        from spec
        The first byte of the bitfield corresponds to piece indices 0 – 7 from high bit to low bit,
        respectively. The next one corresponds to piece indices 8 – 15, etc. Spare bits at the end
        are set to zero.
        """
        byte_array = bytearray()
        for i in range(0, self.num_pieces, 8):
            byte = 0
            for bit_index in range(8): #fill byte
                if i + bit_index < self.num_pieces: #if there is another piece, otherwise bits are already set to 0
                    if self.bits[i + bit_index] == 1: #if we have that piece
                        #https://www.geeksforgeeks.org/python/python-bitwise-operators/
                        #https://stackoverflow.com/questions/21405341/confusion-in-left-shift-operator-in-python
                        #set correct bit position
                        mask = 1 << (7 - bit_index)
                        byte = byte | mask
            byte_array.append(byte)
        return bytes(byte_array)

    @staticmethod #to call from elsewhere use: bf = Bitfield.from_bytes(payload, num_pieces)
    def from_bytes(data: bytes, num_pieces: int):
        bitfield = Bitfield(num_pieces)
        bits = []
        for byte in data:
            for bit_index in range(8):
                bit = (byte >> (7 - bit_index)) & 1
                bits.append(bit)
        bitfield.bits = []
        for i in range(num_pieces): #discard padding
            bitfield.bits.append(bits[i])
        return bitfield

    def _validate(self, index): #prevents out of bounds errors
        if index < 0 or index >= self.num_pieces:
            raise IndexError("Piece index out of range")