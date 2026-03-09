# message.py - Message encoding/decoding for the P2P protocol
# Project spec section 3.2 defines message format: [4-byte length][1-byte type][payload]
import struct
from enum import IntEnum
from typing import Optional, Tuple

# MessageType enum defines all 8 message types from the project spec (section 3.2)
class MessageType(IntEnum):
    CHOKE = 0           # Tell peer we're not uploading to them
    UNCHOKE = 1         # Tell peer we're uploading to them
    INTERESTED = 2      # Tell peer we want pieces from them
    NOT_INTERESTED = 3  # Tell peer we don't want pieces from them
    HAVE = 4            # Tell peer we have a specific piece
    BITFIELD = 5        # Send list of pieces we have (after handshake)
    REQUEST = 6         # Ask peer for a specific piece
    PIECE = 7           # Send a piece to peer

# Take a message type and payload, pack it into the spec format
def encode_message(msg_type: int, payload: bytes = b"") -> bytes:
    # Check that message type is valid (0-7)
    if not 0 <= msg_type <= 7:
        raise ValueError(f"Invalid message type: {msg_type}")
    
    # Combine type byte with payload
    # (spec: message = [1-byte type] + [payload])
    msg_content = bytes([msg_type]) + payload
    
    # Get total length of type + payload
    length = len(msg_content)
    
    # Pack length as 4-byte big-endian integer (network byte order)
    # (spec: first 4 bytes are length, "!" means big-endian, "I" means unsigned int)
    # Then add the message content
    return struct.pack("!I", length) + msg_content

# Try to parse one complete message from buffer
# If message is incomplete, return None and 0 bytes consumed
def decode_message(data: bytes) -> Tuple[Optional[int], Optional[bytes], int]:
    # Need at least 4 bytes to read the length field
    if len(data) < 4:
        return None, None, 0  # Not enough data yet, wait for more
    
    # Extract the length field (first 4 bytes, big-endian)
    # "!I" means: big-endian unsigned int
    length = struct.unpack("!I", data[:4])[0]
    
    # Total bytes needed = 4-byte length + actual message
    total_needed = 4 + length
    
    # Check if we have received the entire message
    if len(data) < total_needed:
        return None, None, 0  # Message still incomplete, need more bytes
    
    # Validate that message length makes sense (at least 1 byte for type)
    if length < 1:
        raise ValueError(f"Invalid message length: {length}")
    
    # Extract message type (byte at position 4)
    msg_type = data[4]
    
    # Extract payload (everything after type byte until end of message)
    payload = data[5:total_needed]
    
    # Return: type, payload, and how many bytes we consumed from the buffer
    return msg_type, payload, total_needed
