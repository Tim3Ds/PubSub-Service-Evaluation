#!/usr/bin/env python3
"""
Unified Receiver - Protocol-agnostic receiver implementation for all services.
Uses protobuf binary serialization for minimal overhead.
"""
import json
import sys
import time
import queue
from abc import ABC, abstractmethod
from typing import Dict, Optional, Callable, Any
import asyncio

# gRPC imports
try:
    import grpc
    from repo_root import get_repo_root
    sys.path.insert(0, str(get_repo_root() / 'grpc' / 'python'))
    import messaging_pb2
    import messaging_pb2_grpc
except ImportError:
    pass # Will fail later if gRPC is used but not installed

from messaging import (
    MessageEnvelope, MessagingStats, MessageType,
    RoutingMode, get_current_time_ms, create_ack
)


class UnifiedReceiver(ABC):
    """Abstract base class for all receivers."""
    
    def __init__(self, receiver_id: int, service_name: str, language: str = "Python"):
        self.receiver_id = receiver_id
        self.service_name = service_name
        self.language = language
        self.stats = MessagingStats()
        self._running = False
        self._connected = False
        self._message_handlers: Dict[MessageType, Callable] = {}
    
    @abstractmethod
    def connect(self) -> bool:
        """Establish connection to the messaging service."""
        pass
    
    @abstractmethod
    def disconnect(self):
        """Close connection to the messaging service."""
        pass
    
    @abstractmethod
    def _receive_raw(self, timeout_ms: float) -> Optional[bytes]:
        """Receive raw message bytes."""
        pass
    
    @abstractmethod
    def _send_raw(self, data: bytes) -> bool:
        """Send raw message bytes."""
        pass
    
    def _create_ack(self, envelope: MessageEnvelope, received: bool = True, status: str = "OK") -> MessageEnvelope:
        """Create an acknowledgment message."""
        return create_ack(envelope, self.receiver_id, received, status)
    
    def _handle_message(self, envelope: MessageEnvelope) -> Optional[MessageEnvelope]:
        """Process incoming message and return ACK."""
        handler = self._message_handlers.get(envelope.message_type)
        if handler:
            try:
                handler(envelope)
            except Exception as e:
                print(f" [ERROR] Handler error: {e}")
                return self._create_ack(envelope, received=False, status=str(e))
        return self._create_ack(envelope)
    
    def receive_and_ack(self, timeout_ms: float = 1000.0) -> Optional[MessageEnvelope]:
        """Receive a message and send acknowledgment."""
        if not self._connected:
            if not self.connect():
                return None
        
        try:
            raw_data = self._receive_raw(timeout_ms)
            if raw_data is None:
                return None
            
            envelope = MessageEnvelope.deserialize(raw_data)
            print(f" [{self.service_name}] [Receiver {self.receiver_id}] Received {envelope.message_id}")
            
            ack = self._handle_message(envelope)
            if ack:
                self._send_raw(ack.serialize())
            
            return envelope
        except Exception as e:
            print(f" [ERROR] Receive failed: {e}")
            return None
    
    def run(self, verbose: bool = True):
        """Run the receiver loop."""
        if not self._connected:
            if not self.connect():
                return
        
        self._running = True
        if verbose:
            print(f" [{self.service_name}] [Receiver {self.receiver_id}] Starting...")
        
        try:
            while self._running:
                self.receive_and_ack(timeout_ms=1000.0)
        except KeyboardInterrupt:
            pass
        finally:
            self.disconnect()
    
    def stop(self):
        """Stop the receiver loop."""
        self._running = False


class RedisReceiver(UnifiedReceiver):
    """Redis receiver implementation using Pub/Sub."""
    
    def __init__(self, receiver_id: int, host: str = 'localhost', port: int = 6379):
        super().__init__(receiver_id, "Redis", "Python")
        self.host = host
        self.port = port
        self._redis = None
        self._pubsub = None
        self._channel_name = f'test_channel_{receiver_id}'
    
    def connect(self) -> bool:
        try:
            import redis
            self._redis = redis.Redis(host=self.host, port=self.port, db=0)
            self._redis.ping()
            self._pubsub = self._redis.pubsub(ignore_subscribe_messages=True)
            self._pubsub.subscribe(self._channel_name)
            self._connected = True
            return True
        except Exception as e:
            print(f" [!] Redis connection failed: {e}")
            return False
    
    def disconnect(self):
        if self._pubsub:
            self._pubsub.unsubscribe(self._channel_name)
            self._pubsub.close()
        if self._redis:
            self._redis.close()
        self._connected = False
    
    def _receive_raw(self, timeout_ms: float) -> Optional[bytes]:
        try:
            # Use get_message with timeout
            message = self._pubsub.get_message(timeout=timeout_ms / 1000.0)
            if message and message['type'] == 'message':
                return message['data']
            return None
        except Exception:
            return None
    
    def _send_raw(self, data: bytes) -> bool:
        try:
            # Parse the message to get reply_to from metadata
            envelope = MessageEnvelope.deserialize(data)
            reply_to = envelope.metadata.get('reply_to')
            if reply_to:
                # Publish acknowledgment to the reply channel
                self._redis.publish(reply_to, data)
                return True
            return False
        except Exception:
            return False


class RabbitMQReceiver(UnifiedReceiver):
    """RabbitMQ receiver implementation."""
    
    def __init__(self, receiver_id: int, host: str = 'localhost', port: int = 5672):
        super().__init__(receiver_id, "RabbitMQ", "Python")
        self.host = host
        self.port = port
        self._connection = None
        self._channel = None
        self._queue_name = f'test_queue_{receiver_id}'
    
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
            self._channel.queue_declare(queue=self._queue_name, passive=False)
            self._connected = True
            return True
        except Exception as e:
            print(f" [!] RabbitMQ connection failed: {e}")
            return False
    
    def disconnect(self):
        if self._connection:
            self._connection.close()
        self._connected = False
    
    def _receive_raw(self, timeout_ms: float) -> Optional[bytes]:
        try:
            method, properties, body = self._channel.basic_get(
                queue=self._queue_name, 
                auto_ack=False
            )
            if body:
                self._channel.basic_ack(delivery_tag=method.delivery_tag)
                
                # Check for reply_to in properties
                if properties.reply_to:
                    try:
                        # Inject reply_to into envelope metadata
                        envelope = MessageEnvelope.deserialize(body)
                        envelope.metadata['reply_to'] = properties.reply_to
                        return envelope.serialize()
                    except Exception:
                        # If deserialization fails, return original body (might fail later)
                        return body
                return body
            return None
        except Exception:
            return None
    
    def _send_raw(self, data: bytes) -> bool:
        try:
            envelope = MessageEnvelope.deserialize(data)
            reply_to = envelope.metadata.get('reply_to')
            if reply_to:
                self._channel.basic_publish(
                    exchange='', 
                    routing_key=reply_to, 
                    body=data
                )
                return True
            return False
        except Exception:
            return False


class ZeroMQReceiver(UnifiedReceiver):
    """ZeroMQ receiver implementation."""
    
    def __init__(self, receiver_id: int):
        super().__init__(receiver_id, "ZeroMQ", "Python")
        self._context = None
        self._socket = None
        self._port = 5556 + receiver_id
    
    def connect(self) -> bool:
        try:
            import zmq
            self._context = zmq.Context()
            self._socket = self._context.socket(zmq.REP)
            self._socket.bind(f"tcp://*:{self._port}")
            self._socket.setsockopt(zmq.RCVTIMEO, 1000)
            self._connected = True
            return True
        except Exception as e:
            print(f" [!] ZeroMQ bind failed: {e}")
            return False
    
    def disconnect(self):
        if self._socket:
            self._socket.close()
        if self._context:
            self._context.term()
        self._connected = False
    
    def _receive_raw(self, timeout_ms: float) -> Optional[bytes]:
        try:
            return self._socket.recv()
        except Exception:
            return None
    
    def _send_raw(self, data: bytes) -> bool:
        try:
            self._socket.send(data)
            return True
        except Exception:
            return False


class NatsReceiver(UnifiedReceiver):
    """NATS receiver implementation."""
    
    def __init__(self, receiver_id: int, host: str = 'localhost', port: int = 4222):
        super().__init__(receiver_id, "NATS", "Python")
        self.host = host
        self.port = port
        self._nc = None
        self._subject = f"test.subject.{receiver_id}"
        self._subscription = None
        self._loop = asyncio.new_event_loop()
        self._last_reply = None
    
    def connect(self) -> bool:
        try:
            import nats
            import asyncio
            asyncio.set_event_loop(self._loop)
            self._nc = self._loop.run_until_complete(nats.connect(f"nats://{self.host}:{self.port}"))
            self._subscription = self._loop.run_until_complete(self._nc.subscribe(self._subject))
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
    
    def _receive_raw(self, timeout_ms: float) -> Optional[bytes]:
        # NATS is push-based, this is a simplified polling implementation
        try:
            import asyncio
            msg = self._loop.run_until_complete(self._subscription.next_msg(timeout=timeout_ms / 1000.0))
            if msg:
                self._last_reply = msg.reply
                return msg.data
            return None
        except Exception:
            return None
    
    def _send_raw(self, data: bytes) -> bool:
        try:
            if self._last_reply:
                self._loop.run_until_complete(self._nc.publish(self._last_reply, data))
                return True
            return False
        except Exception:
            return False


class GrpcReceiver(UnifiedReceiver):
    """gRPC receiver implementation.
    
    Each receiver binds to its own port (50051 + receiver_id) and acts as a gRPC server.
    This allows multiple receivers to run concurrently on different ports.
    """
    
    def __init__(self, receiver_id: int, server_host: str = 'localhost', server_port: int = 50051):
        super().__init__(receiver_id, "gRPC", "Python")
        self.server_host = server_host
        # Port is calculated as base_port + receiver_id to allow multiple receivers
        self.port = server_port + receiver_id
        self._server = None
        self._server_thread = None
        self._messages_queue = queue.Queue()
        self._running = False
        self._channel = None
        self._stub = None
    
    def connect(self) -> bool:
        """Start the gRPC server for this receiver."""
        try:
            import grpc
            from concurrent import futures
            from repo_root import get_repo_root
            sys.path.insert(0, str(get_repo_root() / 'grpc' / 'python'))
            import messaging_pb2
            import messaging_pb2_grpc
            
            # Create gRPC server
            self._server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
            
            # Add listening port
            server_address = f'{self.server_host}:{self.port}'
            self._server.add_insecure_port(server_address)
            
            # Create and register the service
            servicer = _GrpcReceiverServicer(self)
            messaging_pb2_grpc.add_MessagingServiceServicer_to_server(servicer, self._server)
            
            # Start server
            self._server.start()
            self._running = True
            self._connected = True
            
            print(f" [gRPC] Receiver {self.receiver_id} listening on {server_address}")
            return True
        except Exception as e:
            print(f" [!] gRPC server start failed for receiver {self.receiver_id}: {e}")
            return False
    
    def disconnect(self):
        """Stop the gRPC server."""
        self._running = False
        if self._server:
            self._server.stop(grace=5)
        self._connected = False
    
    def _receive_raw(self, timeout_ms: float) -> Optional[bytes]:
        """Receive raw message bytes from the internal queue."""
        try:
            # Non-blocking check with timeout
            import time
            start_time = time.time()
            timeout_seconds = timeout_ms / 1000.0
            
            while time.time() - start_time < timeout_seconds:
                try:
                    # Try to get message with short timeout to allow checking _running
                    msg = self._messages_queue.get_nowait()
                    return msg
                except queue.Empty:
                    if not self._running:
                        return None
                    time.sleep(0.01)  # 10ms sleep to avoid busy waiting
            
            return None
        except Exception as e:
            print(f" [ERROR] gRPC receive failed: {e}")
            return None
    
    def _send_raw(self, data: bytes) -> bool:
        """Send raw message bytes (ACK back to sender)."""
        try:
            # In gRPC, ACKs are sent as responses in the RPC call, not separate sends
            return True
        except Exception:
            return False
    
    def _add_message(self, message_data: bytes):
        """Add received message to internal queue (called by servicer)."""
        try:
            self._messages_queue.put_nowait(message_data)
        except queue.Full:
            print(f" [WARNING] Message queue full for receiver {self.receiver_id}")


class _GrpcReceiverServicer:
    """Internal servicer for gRPC receiver messages."""
    
    def __init__(self, receiver: GrpcReceiver):
        self._receiver = receiver
    
    def SendMessage(self, request, context):
        """Handle incoming message and queue it for processing."""
        try:
            from messaging import create_ack, MessageEnvelope, MessageType
            
            # Queue the raw message for async processing
            self._receiver._add_message(request.SerializeToString())
            
            # Create ACK using the factory function (uses protobuf serialization)
            original_envelope = MessageEnvelope.from_protobuf(request)
            ack_envelope = create_ack(original_envelope, self._receiver.receiver_id, True, "OK")
            
            # Convert to protobuf for response
            response = ack_envelope.to_protobuf()
            
            return response
        except Exception as e:
            print(f" [ERROR] Servicer error: {e}")
            import traceback
            traceback.print_exc()
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(str(e))
            return messaging_pb2.MessageEnvelope()

    def StreamMessages(self, request_iterator, context):
        """Handle bidirectional streaming of messages."""
        for request in request_iterator:
            yield self.SendMessage(request, context)

    def Subscribe(self, request, context):
        """Handle server-side streaming (not fully implemented)."""
        yield self.SendMessage(request, context)


class ActiveMQReceiver(UnifiedReceiver):
    """ActiveMQ receiver implementation."""
    
    def __init__(self, receiver_id: int, host: str = 'localhost', port: int = 61616):
        super().__init__(receiver_id, "ActiveMQ", "Python")
        self.host = host
        self.port = port
        self._connection = None
        self._destination = f"/queue/test.queue.{receiver_id}"
    
    def connect(self) -> bool:
        try:
            import stomp
            self._conn = stomp.Connection([(self.host, self.port)])
            self._conn.connect('admin', 'admin', wait=True)
            self._conn.subscribe(self._destination, id=receiver_id, ack='auto')
            self._connected = True
            return True
        except Exception as e:
            print(f" [!] ActiveMQ connection failed: {e}")
            return False
    
    def disconnect(self):
        if self._connection:
            self._connection.unsubscribe(self._destination)
            self._connection.disconnect()
        self._connected = False
    
    def _receive_raw(self, timeout_ms: float) -> Optional[bytes]:
        # ActiveMQ uses callback-based receiving
        # This is a simplified polling implementation
        try:
            # Would need callback listener for real implementation
            return None
        except Exception:
            return None
    
    def _send_raw(self, data: bytes) -> bool:
        try:
            envelope = MessageEnvelope.deserialize(data)
            # Would send to reply-to destination
            return True
        except Exception:
            return False


def create_receiver(service: str, receiver_id: int, **kwargs) -> UnifiedReceiver:
    """Factory function to create a receiver for the specified service."""
    receivers = {
        'redis': RedisReceiver,
        'rabbitmq': RabbitMQReceiver,
        'zeromq': ZeroMQReceiver,
        'nats': NatsReceiver,
        'grpc': GrpcReceiver,
        'activemq': ActiveMQReceiver,
    }
    
    service_lower = service.lower()
    if service_lower not in receivers:
        raise ValueError(f"Unknown service: {service}")
    
    return receivers[service_lower](receiver_id, **kwargs)

