#!/usr/bin/env python3
"""
Unified Sender - Protocol-agnostic synchronous message sender for all services.
Supports Redis, RabbitMQ, NATS, ZeroMQ, gRPC, and ActiveMQ.
"""
import json
import sys
import time
import asyncio
from abc import ABC, abstractmethod
from typing import Dict, Optional, Any, List
from dataclasses import dataclass

from messaging import (
    MessageEnvelope, MessagingStats, MessageType,
    RoutingMode, get_current_time_ms, create_message_envelope
)


@dataclass
class SendResult:
    """Result of a send operation."""
    success: bool
    message_id: str
    latency_ms: float
    receiver_id: str
    error: Optional[str] = None


class UnifiedSender(ABC):
    """Abstract base class for all senders."""
    
    def __init__(self, service_name: str, language: str = "Python"):
        self.service_name = service_name
        self.language = language
        self.stats = MessagingStats()
        self._connected = False
    
    @abstractmethod
    def connect(self) -> bool:
        """Establish connection to the messaging service."""
        pass
    
    @abstractmethod
    def disconnect(self):
        """Close connection to the messaging service."""
        pass
    
    @abstractmethod
    def _send_raw(self, envelope: MessageEnvelope) -> bool:
        """Send raw message envelope without waiting for response."""
        pass
    
    @abstractmethod
    def _send_with_ack(self, envelope: MessageEnvelope, timeout_ms: float) -> Optional[MessageEnvelope]:
        """Send message and wait for acknowledgment."""
        pass
    
    def send(
        self,
        target: int,
        payload: Any,
        topic: str = "",
        wait_for_ack: bool = True,
        timeout_ms: float = 5000.0,
        metadata: Optional[Dict[str, str]] = None
    ) -> SendResult:
        """Send a message to a target receiver."""
        if not self._connected:
            if not self.connect():
                return SendResult(
                    success=False,
                    message_id="",
                    latency_ms=0,
                    receiver_id=str(target),
                    error="Failed to connect"
                )
        
        envelope = create_message_envelope(
            target=target,
            payload=payload,
            message_type=MessageType.DATA_MESSAGE,
            topic=topic,
            async_flag=False,
            routing=RoutingMode.POINT_TO_POINT,
            metadata=metadata
        )
        
        msg_start = get_current_time_ms()
        
        if wait_for_ack:
            response = self._send_with_ack(envelope, timeout_ms)
            latency_ms = get_current_time_ms() - msg_start
            
            if response and response.message_type == MessageType.ACK:
                # Parse ACK payload using protobuf deserialization
                try:
                    from messaging import Acknowledgment
                    if response.payload:
                        # Deserialize ACK from payload using protobuf (with JSON fallback)
                        ack = Acknowledgment.deserialize(response.payload)
                        return SendResult(
                            success=ack.received,
                            message_id=envelope.message_id,
                            latency_ms=latency_ms,
                            receiver_id=ack.receiver_id if ack.receiver_id else str(target),
                            error=None if ack.received else ack.status
                        )
                    else:
                        # ACK received but no payload data
                        return SendResult(
                            success=True,
                            message_id=envelope.message_id,
                            latency_ms=latency_ms,
                            receiver_id=str(target),
                            error=None
                        )
                except Exception as e:
                    return SendResult(
                        success=False,
                        message_id=envelope.message_id,
                        latency_ms=latency_ms,
                        receiver_id=str(target),
                        error=f"ACK parse error: {str(e)}"
                    )
            else:
                return SendResult(
                    success=False,
                    message_id=envelope.message_id,
                    latency_ms=latency_ms,
                    receiver_id=str(target),
                    error="No acknowledgment received"
                )
        else:
            # Fire and forget
            success = self._send_raw(envelope)
            latency_ms = get_current_time_ms() - msg_start
            # Stats are recorded by caller (run_performance_test)
            return SendResult(
                success=success,
                message_id=envelope.message_id,
                latency_ms=latency_ms,
                receiver_id=str(target),
                error=None if success else "Send failed"
            )
    
    def send_batch(
        self,
        messages: List[Dict[str, Any]],
        wait_for_ack: bool = True,
        timeout_ms: float = 5000.0
    ) -> List[SendResult]:
        """Send a batch of messages."""
        results = []
        for msg in messages:
            target = msg.get('target', 0)
            payload = msg.get('payload', msg)
            topic = msg.get('topic', '')
            metadata = msg.get('metadata', {})
            
            result = self.send(
                target=target,
                payload=payload,
                topic=topic,
                wait_for_ack=wait_for_ack,
                timeout_ms=timeout_ms,
                metadata=metadata
            )
            results.append(result)
        
        return results
    
    def run_performance_test(
        self,
        test_data: List[Dict[str, Any]],
        wait_for_ack: bool = True,
        timeout_ms: float = 5000.0
    ) -> Dict[str, Any]:
        """Run a performance test and return statistics."""
        self.stats = MessagingStats()
        self.stats.start_time = get_current_time_ms()
        
        for msg in test_data:
            target = msg.get('target', 0)
            payload = msg.get('payload', msg)
            topic = msg.get('topic', '')
            
            result = self.send(
                target=target,
                payload=payload,
                topic=topic,
                wait_for_ack=wait_for_ack,
                timeout_ms=timeout_ms
            )
            self.stats.record_send(result.success, result.latency_ms)
        
        self.stats.end_time = get_current_time_ms()
        
        return {
            "service": self.service_name,
            "language": self.language,
            **self.stats.get_stats()
        }


class RedisSender(UnifiedSender):
    """Redis sender implementation using Pub/Sub."""
    
    def __init__(self, host: str = 'localhost', port: int = 6379):
        super().__init__("Redis", "Python")
        self.host = host
        self.port = port
        self._redis = None
    
    def connect(self) -> bool:
        try:
            import redis
            self._redis = redis.Redis(host=self.host, port=self.port, db=0)
            self._redis.ping()
            self._connected = True
            return True
        except Exception as e:
            print(f" [!] Redis connection failed: {e}")
            return False
    
    def disconnect(self):
        if self._redis:
            self._redis.close()
        self._connected = False
    
    def _get_channel_name(self, target: int) -> str:
        return f"test_channel_{target}"
    
    def _get_reply_channel(self, message_id: str) -> str:
        return f"reply_channel_{message_id}"
    
    def _send_raw(self, envelope: MessageEnvelope) -> bool:
        try:
            channel_name = self._get_channel_name(envelope.target)
            data = envelope.serialize()
            
            # Publish with retry if no subscribers (handle race condition in harness)
            for _ in range(5):
                num_receivers = self._redis.publish(channel_name, data)
                if num_receivers > 0:
                    break
                time.sleep(0.2)
            return True
        except Exception:
            return False
    
    def _send_with_ack(self, envelope: MessageEnvelope, timeout_ms: float) -> Optional[MessageEnvelope]:
        try:
            channel_name = self._get_channel_name(envelope.target)
            reply_channel = self._get_reply_channel(envelope.message_id)
            
            # Setup subscription for ACK first
            pubsub = self._redis.pubsub(ignore_subscribe_messages=True)
            pubsub.subscribe(reply_channel)
            
            # Add reply_to in metadata
            envelope.metadata['reply_to'] = reply_channel
            
            data = envelope.serialize()
            # Publish with retry if no subscribers (handle race condition in harness)
            for _ in range(5):
                num_receivers = self._redis.publish(channel_name, data)
                if num_receivers > 0:
                    break
                time.sleep(0.2)
            
            # Wait for response
            start_time = time.time()
            timeout_seconds = timeout_ms / 1000.0
            
            while (time.time() - start_time) < timeout_seconds:
                remaining = timeout_seconds - (time.time() - start_time)
                if remaining <= 0:
                    break
                message = pubsub.get_message(timeout=remaining)
                
                if message and message['type'] == 'message':
                    resp = MessageEnvelope.deserialize(message['data'])
                    pubsub.unsubscribe(reply_channel)
                    pubsub.close()
                    return resp
            
            pubsub.unsubscribe(reply_channel)
            pubsub.close()
            return None
        except Exception:
            return None


class RabbitMQSender(UnifiedSender):
    """RabbitMQ sender implementation."""
    
    def __init__(self, host: str = 'localhost', port: int = 5672):
        super().__init__("RabbitMQ", "Python")
        self.host = host
        self.port = port
        self._connection = None
        self._channel = None
    
    def connect(self) -> bool:
        try:
            import pika
            credentials = pika.PlainCredentials('guest', 'guest')
            parameters = pika.ConnectionParameters(
                host=self.host, 
                port=self.port, 
                credentials=credentials
            )
            self._connection = pika.BlockingConnection(parameters)
            self._channel = self._connection.channel()
            self._connected = True
            return True
        except Exception as e:
            print(f" [!] RabbitMQ connection failed: {e}")
            return False
    
    def disconnect(self):
        if self._connection:
            self._connection.close()
        self._connected = False
    
    def _get_queue_name(self, target: int) -> str:
        return f"test_queue_{target}"
    
    def _send_raw(self, envelope: MessageEnvelope) -> bool:
        try:
            queue_name = self._get_queue_name(envelope.target)
            data = envelope.serialize()
            self._channel.basic_publish(exchange='', routing_key=queue_name, body=data)
            return True
        except Exception:
            return False
    
    def _send_with_ack(self, envelope: MessageEnvelope, timeout_ms: float) -> Optional[MessageEnvelope]:
        try:
            queue_name = self._get_queue_name(envelope.target)
            reply_queue = f"reply_{envelope.message_id}"
            
            # Add reply_to in metadata
            envelope.metadata['reply_to'] = reply_queue
            
            # Declare reply queue
            self._channel.queue_declare(queue=reply_queue, auto_delete=True)
            
            import pika
            data = envelope.serialize()
            self._channel.basic_publish(
                exchange='', 
                routing_key=queue_name, 
                body=data,
                properties=pika.BasicProperties(
                    reply_to=reply_queue,
                    content_type='application/json',
                    delivery_mode=2  # persistent
                )
            )
            
            # Wait for response with basic_get (polling implementation for sync client)
            import time
            start_time = time.time()
            while time.time() - start_time < (timeout_ms / 1000.0):
                method, properties, body = self._channel.basic_get(
                    queue=reply_queue, 
                    auto_ack=True
                )
                if body:
                    # Clean up
                    self._channel.queue_delete(queue=reply_queue)
                    return MessageEnvelope.deserialize(body)
                time.sleep(0.01)
            
            # Clean up on timeout
            self._channel.queue_delete(queue=reply_queue)
            return None
        except Exception:
            return None


class ZeroMQSender(UnifiedSender):
    """ZeroMQ sender implementation."""
    
    def __init__(self):
        super().__init__("ZeroMQ", "Python")
        self._context = None
        self._socket = None
    
    def connect(self) -> bool:
        try:
            import zmq
            self._context = zmq.Context()
            self._socket = self._context.socket(zmq.REQ)
            self._socket.connect("tcp://localhost:5555")
            self._socket.setsockopt(zmq.RCVTIMEO, 5000)
            self._connected = True
            return True
        except Exception as e:
            print(f" [!] ZeroMQ connection failed: {e}")
            return False
    
    def disconnect(self):
        if self._socket:
            self._socket.close()
        if self._context:
            self._context.term()
        self._connected = False
    
    def _get_port(self, target: int) -> int:
        # Each receiver binds to a different port: 5556 + target
        return 5556 + target
    
    def _send_raw(self, envelope: MessageEnvelope) -> bool:
        try:
            import zmq
            # For ZeroMQ, we need to connect to the specific receiver's port
            port = self._get_port(envelope.target)
            socket = self._context.socket(zmq.REQ)
            socket.connect(f"tcp://localhost:{port}")
            socket.setsockopt(zmq.RCVTIMEO, 5000)
            
            data = envelope.serialize()
            socket.send(data)
            # Don't wait for response in fire-and-forget mode
            socket.close()
            return True
        except Exception:
            return False
    
    def _send_with_ack(self, envelope: MessageEnvelope, timeout_ms: float) -> Optional[MessageEnvelope]:
        try:
            import zmq
            port = self._get_port(envelope.target)
            socket = self._context.socket(zmq.REQ)
            socket.connect(f"tcp://localhost:{port}")
            socket.setsockopt(zmq.RCVTIMEO, int(timeout_ms))
            
            data = envelope.serialize()
            socket.send(data)
            
            response_data = socket.recv()
            socket.close()
            
            return MessageEnvelope.deserialize(response_data)
        except Exception:
            return None


class NatsSender(UnifiedSender):
    """NATS sender implementation."""
    
    def __init__(self, host: str = 'localhost', port: int = 4222):
        super().__init__("NATS", "Python")
        self.host = host
        self.port = port
        self._nc = None
        self._loop = asyncio.new_event_loop()
    
    def connect(self) -> bool:
        try:
            import nats
            asyncio.set_event_loop(self._loop)
            self._nc = self._loop.run_until_complete(nats.connect(
                f"nats://{self.host}:{self.port}"
            ))
            self._connected = True
            return True
        except Exception as e:
            print(f" [!] NATS connection failed: {e}")
            return False
    
    def disconnect(self):
        if self._nc:
            self._loop.run_until_complete(self._nc.close())
        self._connected = False
        self._loop.close()
    
    def _get_subject(self, target: int) -> str:
        return f"test.subject.{target}"
    
    def _send_raw(self, envelope: MessageEnvelope) -> bool:
        try:
            subject = self._get_subject(envelope.target)
            data = envelope.serialize()
            self._loop.run_until_complete(self._nc.publish(subject, data))
            return True
        except Exception:
            return False
    
    def _send_with_ack(self, envelope: MessageEnvelope, timeout_ms: float) -> Optional[MessageEnvelope]:
        # NATS uses inbox for request-reply
        try:
            inbox = f"inbox_{envelope.message_id}"
            subject = self._get_subject(envelope.target)
            data = envelope.serialize()
            
            msg = self._loop.run_until_complete(self._nc.request(subject, data, timeout=timeout_ms / 1000.0))
            if msg:
                return MessageEnvelope.deserialize(msg.data)
            return None
        except Exception:
            return None


class GrpcSender(UnifiedSender):
    """gRPC sender implementation.
    
    Supports connecting to multiple receivers (one per port) for load balancing.
    Each receiver binds to port 50051 + receiver_id, so sender connects to those ports.
    """
    
    def __init__(self, base_port: int = 50051, num_receivers: int = 32):
        super().__init__("gRPC", "Python")
        self.base_port = base_port
        self.num_receivers = num_receivers
        self._channels = {}  # Map receiver_id -> channel
        self._stubs = {}     # Map receiver_id -> stub
        self._available_receivers = []  # Track which receivers are actually available
    
    def connect(self) -> bool:
        """Connect to available receiver ports for load balancing."""
        try:
            import grpc
            from repo_root import get_repo_root
            sys.path.insert(0, str(get_repo_root() / 'utils' / 'python'))
            import messaging_pb2
            import messaging_pb2_grpc
            
            # Try to connect to each receiver port, but only add successful connections
            self._available_receivers = []
            for receiver_id in range(self.num_receivers):
                port = self.base_port + receiver_id
                try:
                    channel = grpc.insecure_channel(f'localhost:{port}')
                    # Use a short timeout to check if receiver is available
                    grpc.channel_ready_future(channel).result(timeout=0.5)
                    stub = messaging_pb2_grpc.MessagingServiceStub(channel)
                    self._channels[receiver_id] = channel
                    self._stubs[receiver_id] = stub
                    self._available_receivers.append(receiver_id)
                except grpc.RpcError:
                    # Receiver not available at this port, skip it
                    continue
                except Exception:
                    # Other error, skip this receiver
                    continue
            
            if not self._available_receivers:
                print(f" [!] gRPC connection failed: No receivers available")
                return False
            
            self._connected = True
            print(f" [gRPC] Connected to {len(self._available_receivers)}/{self.num_receivers} receivers on ports {self.base_port}-{self.base_port + self.num_receivers - 1}")
            return True
        except Exception as e:
            print(f" [!] gRPC connection failed: {e}")
            return False
    
    def disconnect(self):
        """Close all channels."""
        for channel in self._channels.values():
            channel.close()
        self._channels.clear()
        self._stubs.clear()
        self._connected = False
    
    def _get_receiver_for_target(self, target: int) -> int:
        """Get the receiver ID to use for a given target."""
        if not self._available_receivers:
            return 0  # Fallback to receiver 0
        # Round-robin load balancing across available receivers
        # Use modulo to wrap around available receivers
        receiver_index = target % len(self._available_receivers)
        return self._available_receivers[receiver_index]
    
    def _send_raw(self, envelope: MessageEnvelope) -> bool:
        try:
            import messaging_pb2
            receiver_id = self._get_receiver_for_target(envelope.target)
            stub = self._stubs.get(receiver_id)
            if stub:
                proto_env = envelope.to_protobuf()
                stub.SendMessage(proto_env)
                return True
            return False
        except Exception:
            return False
    
    def _send_with_ack(self, envelope: MessageEnvelope, timeout_ms: float) -> Optional[MessageEnvelope]:
        try:
            import messaging_pb2
            receiver_id = self._get_receiver_for_target(envelope.target)
            stub = self._stubs.get(receiver_id)
            if stub:
                proto_env = envelope.to_protobuf()
                response = stub.SendMessage(proto_env, timeout=timeout_ms / 1000.0)
                return MessageEnvelope.from_protobuf(response)
            return None
        except Exception as e:
            print(f" [DEBUG] gRPC Send failed: {e}")
            return None


class ActiveMQSender(UnifiedSender):
    """ActiveMQ sender implementation."""
    
    def __init__(self, host: str = 'localhost', port: int = 61616):
        super().__init__("ActiveMQ", "Python")
        self.host = host
        self.port = port
        self._connection = None
        self._session = None
        self._producer = None
    
    def connect(self) -> bool:
        try:
            import stomp
            self._conn = stomp.Connection([(self.host, self.port)])
            self._conn.connect('admin', 'admin', wait=True)
            self._connected = True
            return True
        except Exception as e:
            print(f" [!] ActiveMQ connection failed: {e}")
            return False
    
    def disconnect(self):
        if self._connection:
            self._connection.disconnect()
        self._connected = False
    
    def _get_destination(self, target: int) -> str:
        return f"/queue/test.queue.{target}"
    
    def _send_raw(self, envelope: MessageEnvelope) -> bool:
        try:
            destination = self._get_destination(envelope.target)
            data = envelope.serialize()
            self._conn.send(destination, data)
            return True
        except Exception:
            return False
    
    def _send_with_ack(self, envelope: MessageEnvelope, timeout_ms: float) -> Optional[MessageEnvelope]:
        # ActiveMQ has built-in receipt mechanism
        try:
            import stomp
            destination = self._get_destination(envelope.target)
            data = envelope.serialize()
            
            # Subscribe to temp queue for response
            receipt_id = f"receipt_{envelope.message_id}"
            self._conn.send(destination, data, receipt=receipt_id)
            
            # Wait for receipt (simplified - real impl needs callback)
            time.sleep(timeout_ms / 1000.0)
            return None  # Would need async callback for real ACK
        except Exception:
            return None


def create_sender(service: str, **kwargs) -> UnifiedSender:
    """Factory function to create a sender for the specified service."""
    senders = {
        'redis': RedisSender,
        'rabbitmq': RabbitMQSender,
        'zeromq': ZeroMQSender,
        'nats': NatsSender,
        'grpc': GrpcSender,
        'activemq': ActiveMQSender,
    }
    
    service_lower = service.lower()
    if service_lower not in senders:
        raise ValueError(f"Unknown service: {service}")
    
    return senders[service_lower](**kwargs)

