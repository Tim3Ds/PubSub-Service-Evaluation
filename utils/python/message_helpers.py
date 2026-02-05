"""
Message Helper Utilities for Python
Provides clean helper functions for protobuf MessageEnvelope operations.
Mirrors the C++ message_helpers.hpp functionality.
"""
import time
import sys
from pathlib import Path

# Add utils/python to path if not already there
repo_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(repo_root / 'utils' / 'python'))

from messaging_pb2 import MessageEnvelope, DataMessage, Acknowledgment, MessageType, RoutingMode


def get_current_time_ms() -> int:
    """Get current time in milliseconds."""
    return int(time.time() * 1000)


def extract_message_id(item: dict) -> str:
    """Safely extract message_id from test data item."""
    msg_id = item.get('message_id', '')
    if isinstance(msg_id, (int, float)):
        return str(int(msg_id))
    return str(msg_id)


def create_data_envelope(
    item: dict, 
    routing: RoutingMode = RoutingMode.POINT_TO_POINT,
    metadata: dict = None
) -> MessageEnvelope:
    """Create a MessageEnvelope from test data JSON with DataMessage payload."""
    envelope = MessageEnvelope()
    envelope.message_id = extract_message_id(item)
    envelope.target = item.get('target', 0)
    envelope.type = MessageType.DATA_MESSAGE
    envelope.timestamp = get_current_time_ms()
    envelope.routing = routing
    envelope.qos = 1
    setattr(envelope, 'async', False)
    
    # Set metadata
    if metadata:
        for k, v in metadata.items():
            envelope.metadata[k] = str(v)
    
    if 'metadata' in item and isinstance(item['metadata'], dict):
        for k, v in item['metadata'].items():
            envelope.metadata[k] = str(v)
    
    # Create DataMessage payload
    data_msg = DataMessage()
    data_msg.message_name = item.get('message_name', item.get('topic', ''))
    
    # Handle message_value array
    if 'message_value' in item:
        values = item['message_value']
        if isinstance(values, list):
            for val in values:
                data_msg.message_value.append(str(val))
        else:
            data_msg.message_value.append(str(values))
    
    # Serialize DataMessage into payload
    envelope.payload = data_msg.SerializeToString()
    
    return envelope


def create_ack_envelope(
    original_message_id: str,
    target: int,
    receiver_id: str,
    status: str = "OK",
    latency_ms: float = 0.5
) -> MessageEnvelope:
    """Create an ACK MessageEnvelope."""
    envelope = MessageEnvelope()
    envelope.message_id = f"ack_{original_message_id}"
    envelope.target = target
    envelope.type = MessageType.ACK
    envelope.timestamp = get_current_time_ms()
    envelope.routing = RoutingMode.REQUEST_REPLY
    envelope.qos = 1
    
    # Populate direct Acknowledgment field
    envelope.ack.original_message_id = original_message_id
    envelope.ack.received = (status == "OK")
    envelope.ack.latency_ms = latency_ms
    envelope.ack.receiver_id = receiver_id
    envelope.ack.status = status
    setattr(envelope, 'async', False)
    
    return envelope


def create_ack_from_envelope(msg_envelope: MessageEnvelope, receiver_id: str) -> MessageEnvelope:
    """Create an ACK MessageEnvelope from a received message envelope."""
    return create_ack_envelope(
        original_message_id=msg_envelope.message_id,
        target=msg_envelope.target,
        receiver_id=receiver_id,
        status="OK",
        latency_ms=0.5
    )


def parse_envelope(data: bytes) -> MessageEnvelope:
    """Parse a MessageEnvelope from binary data."""
    envelope = MessageEnvelope()
    envelope.ParseFromString(data)
    return envelope


def serialize_envelope(envelope: MessageEnvelope) -> bytes:
    """Serialize a MessageEnvelope to binary data."""
    return envelope.SerializeToString()


def is_valid_ack(envelope: MessageEnvelope, expected_message_id: str) -> bool:
    """Check if an envelope is a valid ACK for the given message_id."""
    if not envelope.HasField('ack'):
        # Fallback for old style where it might be in payload
        try:
            ack = Acknowledgment()
            ack.ParseFromString(envelope.payload)
            return ack.original_message_id == expected_message_id and ack.received
        except Exception:
            return False
            
    return envelope.ack.original_message_id == expected_message_id and envelope.ack.received
