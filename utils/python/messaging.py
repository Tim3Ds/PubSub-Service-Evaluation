#!/usr/bin/env python3
"""
Unified Messaging Core - Protocol-agnostic messaging for all services.
Uses JSON serialization for compatibility across all languages.
"""
import uuid
import json
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from enum import IntEnum


class MessageType(IntEnum):
    MESSAGE_TYPE_UNSPECIFIED = 0
    DATA_MESSAGE = 1
    RPC_REQUEST = 2
    RPC_RESPONSE = 3
    ACK = 4
    CONTROL = 5
    EVENT = 6


class RoutingMode(IntEnum):
    ROUTING_UNSPECIFIED = 0
    POINT_TO_POINT = 1
    PUBLISH_SUBSCRIBE = 2
    REQUEST_REPLY = 3
    FANOUT = 4


class QoSLevel(IntEnum):
    QOS_UNSPECIFIED = 0
    AT_MOST_ONCE = 1
    AT_LEAST_ONCE = 2
    EXACTLY_ONCE = 3


@dataclass
class MessageEnvelope:
    """Unified message envelope for all services."""
    message_id: str = ""
    target: int = 0
    topic: str = ""
    message_type: MessageType = MessageType.DATA_MESSAGE
    payload: bytes = b""
    async_flag: bool = False
    timestamp: int = 0
    routing: RoutingMode = RoutingMode.POINT_TO_POINT
    qos: QoSLevel = QoSLevel.AT_MOST_ONCE
    metadata: Dict[str, str] = field(default_factory=dict)
    
    def __post_init__(self):
        if not self.message_id:
            self.message_id = str(uuid.uuid4())
        if not self.timestamp:
            self.timestamp = int(time.time() * 1000)
    
    def serialize(self) -> bytes:
        """Serialize to bytes (JSON format)."""
        return self.to_json().encode('utf-8')
    
    @classmethod
    def deserialize(cls, data: bytes) -> 'MessageEnvelope':
        """Deserialize from bytes."""
        return cls.from_json(data.decode('utf-8') if isinstance(data, bytes) else data)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "message_id": self.message_id,
            "target": self.target,
            "topic": self.topic,
            "type": int(self.message_type),
            "payload": list(self.payload) if isinstance(self.payload, (list, bytes)) else self.payload,
            "async": self.async_flag,
            "timestamp": self.timestamp,
            "routing": int(self.routing),
            "qos": int(self.qos),
            "metadata": self.metadata
        }
    
    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), separators=(",", ":"))
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MessageEnvelope':
        """Create from dictionary."""
        payload = data.get("payload", b"")
        if isinstance(payload, list):
            payload = bytes(payload)
        elif isinstance(payload, str):
            payload = payload.encode('utf-8')
        
        return cls(
            message_id=data.get("message_id", ""),
            target=data.get("target", 0),
            topic=data.get("topic", ""),
            message_type=MessageType(data.get("type", MessageType.DATA_MESSAGE)),
            payload=payload,
            async_flag=data.get("async", False),
            timestamp=data.get("timestamp", 0),
            routing=RoutingMode(data.get("routing", RoutingMode.POINT_TO_POINT)),
            qos=QoSLevel(data.get("qos", QoSLevel.AT_MOST_ONCE)),
            metadata=data.get("metadata", {})
        )
    
    @classmethod
    def from_json(cls, json_str: str) -> 'MessageEnvelope':
        """Create from JSON string."""
        return cls.from_dict(json.loads(json_str))
    
    def to_protobuf(self):
        """Convert to Protobuf message."""
        import sys
        from repo_root import get_repo_root
        # Ensure path is available for import
        repo_root = get_repo_root()
        grpc_python_path = str(repo_root / 'grpc' / 'python')
        if grpc_python_path not in sys.path:
            sys.path.insert(0, grpc_python_path)
        try:
            import messaging_pb2
        except ImportError:
            # Fallback if generated file not found (e.g. in tests without build)
            return None
            
        envelope = messaging_pb2.MessageEnvelope()
        envelope.message_id = self.message_id
        envelope.target = self.target
        envelope.topic = self.topic
        envelope.type = int(self.message_type)
        envelope.payload = self.payload if isinstance(self.payload, bytes) else str(self.payload).encode('utf-8')
        setattr(envelope, 'async', self.async_flag)
        envelope.timestamp = self.timestamp
        envelope.routing = int(self.routing)
        envelope.qos = int(self.qos)
        for k, v in self.metadata.items():
            envelope.metadata[k] = v
        
        return envelope

    @classmethod
    def from_protobuf(cls, proto) -> 'MessageEnvelope':
        """Create from Protobuf message."""
        return cls(
            message_id=proto.message_id,
            target=proto.target,
            topic=proto.topic,
            message_type=MessageType(proto.type),
            payload=proto.payload,
            async_flag=getattr(proto, 'async'),
            timestamp=proto.timestamp,
            routing=RoutingMode(proto.routing),
            qos=QoSLevel(proto.qos),
            metadata=dict(proto.metadata)
        )


@dataclass
class Acknowledgment:
    """Acknowledgment message for message delivery confirmation."""
    original_message_id: str = ""
    received: bool = False
    latency_ms: float = 0.0
    receiver_id: str = ""
    status: str = "OK"
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "original_message_id": self.original_message_id,
            "received": self.received,
            "latency_ms": self.latency_ms,
            "receiver_id": self.receiver_id,
            "status": self.status
        }
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict())
    
    def serialize(self) -> bytes:
        """Serialize to bytes using protobuf."""
        proto = self.to_protobuf()
        if proto:
            return proto.SerializeToString()
        # Fallback to JSON if proto not available
        return self.to_json().encode('utf-8')
    
    @classmethod
    def deserialize(cls, data: bytes) -> 'Acknowledgment':
        """Deserialize from bytes (protobuf or JSON fallback)."""
        try:
            # Try protobuf first
            import sys
            from repo_root import get_repo_root
            repo_root = get_repo_root()
            grpc_python_path = str(repo_root / 'grpc' / 'python')
            if grpc_python_path not in sys.path:
                sys.path.insert(0, grpc_python_path)
            import messaging_pb2
            proto = messaging_pb2.Acknowledgment()
            proto.ParseFromString(data)
            return cls.from_protobuf(proto)
        except Exception:
            # Fallback to JSON
            return cls.from_dict(json.loads(data.decode('utf-8')))
    
    def to_protobuf(self):
        """Convert to Protobuf Acknowledgment message."""
        import sys
        from repo_root import get_repo_root
        repo_root = get_repo_root()
        grpc_python_path = str(repo_root / 'grpc' / 'python')
        if grpc_python_path not in sys.path:
            sys.path.insert(0, grpc_python_path)
        try:
            import messaging_pb2
        except ImportError:
            return None
            
        ack = messaging_pb2.Acknowledgment()
        ack.original_message_id = self.original_message_id
        ack.received = self.received
        ack.latency_ms = self.latency_ms
        ack.receiver_id = self.receiver_id
        ack.status = self.status
        return ack
    
    @classmethod
    def from_protobuf(cls, proto) -> 'Acknowledgment':
        """Create from Protobuf Acknowledgment message."""
        return cls(
            original_message_id=proto.original_message_id,
            received=proto.received,
            latency_ms=proto.latency_ms,
            receiver_id=proto.receiver_id,
            status=proto.status
        )
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Acknowledgment':
        return cls(
            original_message_id=data.get("original_message_id", ""),
            received=data.get("received", False),
            latency_ms=data.get("latency_ms", 0.0),
            receiver_id=data.get("receiver_id", ""),
            status=data.get("status", "OK")
        )


class MessagingStats:
    """Statistics collector for messaging performance metrics."""
    
    def __init__(self):
        self.sent_count = 0
        self.received_count = 0
        self.failed_count = 0
        self.message_timings: List[float] = []
        self.start_time = 0
        self.end_time = 0
    
    def record_send(self, success: bool, timing_ms: float = 0.0):
        """Record a message send attempt."""
        self.sent_count += 1
        if success:
            self.received_count += 1
            if timing_ms > 0:
                self.message_timings.append(timing_ms)
        else:
            self.failed_count += 1
    
    def set_duration(self, start_ms: float, end_ms: float):
        """Set test duration."""
        self.start_time = start_ms
        self.end_time = end_ms
    
    def get_duration_ms(self) -> float:
        """Get test duration in milliseconds."""
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return 0.0
    
    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive statistics."""
        duration = self.get_duration_ms()
        timings = self.message_timings
        
        stats = {
            "total_sent": self.sent_count,
            "total_received": self.received_count,
            "total_failed": self.failed_count,
            "duration_ms": duration,
            "messages_per_ms": (self.received_count / duration) if duration > 0 else 0
        }
        
        if timings:
            stats["message_timing_stats"] = {
                "min_ms": min(timings),
                "max_ms": max(timings),
                "mean_ms": sum(timings) / len(timings),
                "count": len(timings)
            }
        
        return stats


def get_current_time_ms() -> float:
    """Get current timestamp in milliseconds."""
    return time.time() * 1000


def create_message_envelope(
    target: int,
    payload: Any,
    message_type: MessageType = MessageType.DATA_MESSAGE,
    topic: str = "",
    async_flag: bool = False,
    routing: RoutingMode = RoutingMode.POINT_TO_POINT,
    qos: QoSLevel = QoSLevel.AT_MOST_ONCE,
    metadata: Optional[Dict[str, str]] = None
) -> MessageEnvelope:
    """Factory function to create a MessageEnvelope with common defaults."""
    if isinstance(payload, dict):
        payload_bytes = json.dumps(payload).encode('utf-8')
    elif isinstance(payload, str):
        payload_bytes = payload.encode('utf-8')
    elif isinstance(payload, bytes):
        payload_bytes = payload
    else:
        payload_bytes = str(payload).encode('utf-8')
    
    return MessageEnvelope(
        target=target,
        topic=topic,
        message_type=message_type,
        payload=payload_bytes,
        async_flag=async_flag,
        routing=routing,
        qos=qos,
        metadata=metadata or {}
    )


def create_ack(
    original_envelope: MessageEnvelope,
    receiver_id: int,
    received: bool = True,
    status: str = "OK"
) -> MessageEnvelope:
    """Factory function to create an acknowledgment message using protobuf serialization.
    
    The ACK payload is serialized using protobuf for consistent binary encoding.
    """
    latency_ms = get_current_time_ms() - original_envelope.timestamp
    
    # Create Acknowledgment object
    ack = Acknowledgment(
        original_message_id=original_envelope.message_id,
        received=received,
        latency_ms=latency_ms,
        receiver_id=str(receiver_id),
        status=status
    )
    
    # Serialize ACK using protobuf into payload
    ack_payload = ack.serialize()
    
    # Copy reply_to from original message metadata
    metadata = {}
    if 'reply_to' in original_envelope.metadata:
        metadata['reply_to'] = original_envelope.metadata['reply_to']
    
    return MessageEnvelope(
        message_id=f"ack_{original_envelope.message_id}",
        target=original_envelope.target,
        message_type=MessageType.ACK,
        payload=ack_payload,  # Protobuf-serialized Acknowledgment
        routing=RoutingMode.REQUEST_REPLY,
        metadata=metadata
    )

