#!/usr/bin/env python3
"""
Unified Async Sender - Protocol-agnostic asynchronous message sender for all services.
Uses asyncio for concurrent message sending.
"""
import asyncio
import json
import time
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


class UnifiedAsyncSender(ABC):
    """Abstract base class for all async senders."""
    
    def __init__(self, service_name: str, language: str = "Python"):
        self.service_name = service_name
        self.language = language
        self.stats = MessagingStats()
        self._connected = False
        self._max_concurrent = 100
        self._semaphore = None
    
    @abstractmethod
    async def connect(self) -> bool:
        """Establish connection to the messaging service."""
        pass
    
    @abstractmethod
    async def disconnect(self):
        """Close connection to the messaging service."""
        pass
    
    @abstractmethod
    async def _send_raw(self, envelope: MessageEnvelope) -> bool:
        """Send raw message envelope without waiting for response."""
        pass
    
    @abstractmethod
    async def _send_with_ack(self, envelope: MessageEnvelope, timeout_ms: float) -> Optional[MessageEnvelope]:
        """Send message and wait for acknowledgment."""
        pass
    
    def set_concurrency(self, max_concurrent: int):
        """Set maximum concurrent operations."""
        self._max_concurrent = max_concurrent
        self._semaphore = asyncio.Semaphore(max_concurrent)
    
    async def send(
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
            if not await self.connect():
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
            async_flag=True,
            routing=RoutingMode.POINT_TO_POINT,
            metadata=metadata
        )
        
        msg_start = get_current_time_ms()
        
        if self._semaphore:
            async with self._semaphore:
                if wait_for_ack:
                    response = await self._send_with_ack(envelope, timeout_ms)
                else:
                    success = await self._send_raw(envelope)
                    response = None
        else:
            if wait_for_ack:
                response = await self._send_with_ack(envelope, timeout_ms)
            else:
                success = await self._send_raw(envelope)
                response = None
        
        latency_ms = get_current_time_ms() - msg_start
        
        if wait_for_ack:
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
                        self.stats.record_send(False, latency_ms)
                        return SendResult(
                            success=True,
                            message_id=envelope.message_id,
                            latency_ms=latency_ms,
                            receiver_id=str(target),
                            error=None
                        )
                except Exception as e:
                    self.stats.record_send(False, latency_ms)
                    return SendResult(
                        success=False,
                        message_id=envelope.message_id,
                        latency_ms=latency_ms,
                        receiver_id=str(target),
                        error=f"ACK parse error: {str(e)}"
                    )
            else:
                self.stats.record_send(False, latency_ms)
                return SendResult(
                    success=False,
                    message_id=envelope.message_id,
                    latency_ms=latency_ms,
                    receiver_id=str(target),
                    error="No acknowledgment received"
                )
        else:
            self.stats.record_send(success, latency_ms)
            return SendResult(
                success=success,
                message_id=envelope.message_id,
                latency_ms=latency_ms,
                receiver_id=str(target),
                error=None if success else "Send failed"
            )
    
    async def send_batch(
        self,
        messages: List[Dict[str, Any]],
        wait_for_ack: bool = True,
        timeout_ms: float = 5000.0,
        batch_size: int = 100
    ) -> List[SendResult]:
        """Send a batch of messages concurrently."""
        results = []
        
        # Create tasks
        tasks = []
        for msg in messages:
            target = msg.get('target', 0)
            payload = msg.get('payload', msg)
            topic = msg.get('topic', '')
            metadata = msg.get('metadata', {})
            
            task = self.send(
                target=target,
                payload=payload,
                topic=topic,
                wait_for_ack=wait_for_ack,
                timeout_ms=timeout_ms,
                metadata=metadata
            )
            tasks.append(task)
        
        # Process in batches to avoid overwhelming the system
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i + batch_size]
            batch_results = await asyncio.gather(*batch, return_exceptions=True)
            
            for result in batch_results:
                if isinstance(result, Exception):
                    # Handle exception as failed send
                    results.append(SendResult(
                        success=False,
                        message_id="",
                        latency_ms=0,
                        receiver_id="",
                        error=str(result)
                    ))
                else:
                    results.append(result)
        
        return results
    
    async def run_performance_test(
        self,
        test_data: List[Dict[str, Any]],
        wait_for_ack: bool = True,
        timeout_ms: float = 5000.0,
        batch_size: int = 100
    ) -> Dict[str, Any]:
        """Run a performance test and return statistics."""
        self.stats = MessagingStats()
        self.stats.start_time = get_current_time_ms()
        
        results = await self.send_batch(
            messages=test_data,
            wait_for_ack=wait_for_ack,
            timeout_ms=timeout_ms,
            batch_size=batch_size
        )
        
        for result in results:
            self.stats.record_send(result.success, result.latency_ms)
        
        self.stats.end_time = get_current_time_ms()
        
        return {
            "service": self.service_name,
            "language": self.language,
            "async": True,
            **self.stats.get_stats()
        }


class RedisAsyncSender(UnifiedAsyncSender):
    """Redis async sender implementation using Pub/Sub."""
    
    def __init__(self, host: str = 'localhost', port: int = 6379):
        super().__init__("Redis", "Python")
        self.host = host
        self.port = port
        self._redis = None
    
    async def connect(self) -> bool:
        try:
            import redis.asyncio as redis
            self._redis = redis.Redis(host=self.host, port=self.port, db=0)
            await self._redis.ping()
            self._connected = True
            self.set_concurrency(100)
            return True
        except Exception as e:
            print(f" [!] Redis async connection failed: {e}")
            return False
    
    async def disconnect(self):
        if self._redis:
            await self._redis.aclose()
        self._connected = False
    
    def _get_channel_name(self, target: int) -> str:
        return f"test_channel_{target}"
    
    def _get_reply_channel(self, message_id: str) -> str:
        return f"reply_channel_{message_id}"
    
    async def _send_raw(self, envelope: MessageEnvelope) -> bool:
        try:
            channel_name = self._get_channel_name(envelope.target)
            data = envelope.serialize()
            
            # Publish with retry if no subscribers (handle race condition in harness)
            for _ in range(5):
                num_receivers = await self._redis.publish(channel_name, data)
                if num_receivers > 0:
                    break
                await asyncio.sleep(0.2)
            return True
        except Exception:
            return False
    
    async def _send_with_ack(self, envelope: MessageEnvelope, timeout_ms: float) -> Optional[MessageEnvelope]:
        try:
            channel_name = self._get_channel_name(envelope.target)
            reply_channel = self._get_reply_channel(envelope.message_id)
            
            # Setup subscription for ACK
            async with self._redis.pubsub(ignore_subscribe_messages=True) as pubsub:
                await pubsub.subscribe(reply_channel)
                
                # Add reply_to in metadata
                envelope.metadata['reply_to'] = reply_channel
                
                data = envelope.serialize()
                # Publish with retry if no subscribers (handle race condition in harness)
                for _ in range(5):
                    num_receivers = await self._redis.publish(channel_name, data)
                    if num_receivers > 0:
                        break
                    await asyncio.sleep(0.2)
                
                # Wait for response
                start_time = time.time()
                timeout_seconds = timeout_ms / 1000.0
                
                while (time.time() - start_time) < timeout_seconds:
                    remaining = timeout_seconds - (time.time() - start_time)
                    if remaining <= 0:
                        break
                        
                    try:
                        # In redis.asyncio, get_message with timeout
                        message = await asyncio.wait_for(
                            pubsub.get_message(timeout=remaining),
                            timeout=remaining + 0.1
                        )
                        
                        if message and message['type'] == 'message':
                            resp = MessageEnvelope.deserialize(message['data'])
                            await pubsub.unsubscribe(reply_channel)
                            return resp
                    except asyncio.TimeoutError:
                        break
                
                await pubsub.unsubscribe(reply_channel)
            return None
        except Exception:
            return None


class RabbitMQAsyncSender(UnifiedAsyncSender):
    """RabbitMQ async sender implementation."""
    
    def __init__(self, host: str = 'localhost', port: int = 5672):
        super().__init__("RabbitMQ", "Python")
        self.host = host
        self.port = port
        self._connection = None
        self._channel = None
    
    async def connect(self) -> bool:
        try:
            import aio_pika
            self._connection = await aio_pika.connect_robust(
                host=self.host, port=self.port
            )
            self._channel = await self._connection.channel()
            self._connected = True
            self.set_concurrency(100)
            
            # Setup response queue for ACKs if needed
            self._callback_queue = await self._channel.declare_queue(exclusive=True)
            self._futures = {}
            
            self._futures = {}
            
            async def on_response(message: aio_pika.IncomingMessage):
                async with message.process():
                    # Parse envelope to get message ID
                    try:
                        ack_env = MessageEnvelope.deserialize(message.body)
                        # Expecting ack_{original_id}
                        if ack_env.message_id.startswith("ack_"):
                            original_id = ack_env.message_id[4:]
                            if original_id in self._futures:
                                future = self._futures.pop(original_id)
                                future.set_result(message.body)
                    except Exception:
                        pass # Ignore malformed

            await self._callback_queue.consume(on_response)
            
            return True
        except Exception as e:
            print(f" [!] RabbitMQ async connection failed: {e}")
            return False
    
    async def disconnect(self):
        if self._connection:
            await self._connection.close()
        self._connected = False
    
    def _get_queue_name(self, target: int) -> str:
        return f"test_queue_{target}"
    
    async def _send_raw(self, envelope: MessageEnvelope) -> bool:
        try:
            import aio_pika
            queue_name = self._get_queue_name(envelope.target)
            
            message = aio_pika.Message(
                body=envelope.serialize(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            )
            
            await self._channel.default_exchange.publish(
                message,
                routing_key=queue_name
            )
            return True
        except Exception:
            return False
    
    async def _send_with_ack(self, envelope: MessageEnvelope, timeout_ms: float) -> Optional[MessageEnvelope]:
        try:
            import aio_pika
            queue_name = self._get_queue_name(envelope.target)
            correlation_id = envelope.message_id
            
            future = asyncio.get_running_loop().create_future()
            self._futures[correlation_id] = future
            
            message = aio_pika.Message(
                body=envelope.serialize(),
                correlation_id=correlation_id,
                reply_to=self._callback_queue.name,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            )
            
            await self._channel.default_exchange.publish(
                message,
                routing_key=queue_name
            )
            
            try:
                response_data = await asyncio.wait_for(future, timeout=timeout_ms / 1000.0)
                return MessageEnvelope.deserialize(response_data)
            except asyncio.TimeoutError:
                self._futures.pop(correlation_id, None)
                return None
                
        except Exception:
            return None


class ZeroMQAsyncSender(UnifiedAsyncSender):
    """ZeroMQ async sender implementation."""
    
    def __init__(self):
        super().__init__("ZeroMQ", "Python")
        self._context = None
        self._socket = None
    
    async def connect(self) -> bool:
        try:
            import zmq.asyncio
            self._context = zmq.asyncio.Context()
            self._socket = self._context.socket(zmq.REQ)
            self._socket.connect("tcp://localhost:5555")
            self._connected = True
            self.set_concurrency(100)
            return True
        except Exception as e:
            print(f" [!] ZeroMQ async connection failed: {e}")
            return False
    
    async def disconnect(self):
        if self._socket:
            self._socket.close()
        if self._context:
            self._context.term()
        self._connected = False
    
    def _get_port(self, target: int) -> int:
        return 5556 + target
    
    async def _send_raw(self, envelope: MessageEnvelope) -> bool:
        try:
            import zmq
            port = self._get_port(envelope.target)
            socket = self._context.socket(zmq.REQ)
            socket.connect(f"tcp://localhost:{port}")
            data = envelope.serialize()
            await socket.send(data)
            socket.close()
            return True
        except Exception:
            return False
    
    async def _send_with_ack(self, envelope: MessageEnvelope, timeout_ms: float) -> Optional[MessageEnvelope]:
        try:
            import zmq
            port = self._get_port(envelope.target)
            socket = self._context.socket(zmq.REQ)
            socket.connect(f"tcp://localhost:{port}")
            socket.setsockopt(zmq.RCVTIMEO, int(timeout_ms))
            
            data = envelope.serialize()
            await socket.send(data)
            
            response_data = await socket.recv()
            socket.close()
            
            print(f" [DEBUG] Received raw: {response_data}")
            return MessageEnvelope.deserialize(response_data)
        except Exception as e:
            print(f" [DEBUG] ZeroMQ Send/Recv failed: {e}")
            import traceback
            traceback.print_exc()
            return None


class NatsAsyncSender(UnifiedAsyncSender):
    """NATS async sender implementation."""
    
    def __init__(self, host: str = 'localhost', port: int = 4222):
        super().__init__("NATS", "Python")
        self.host = host
        self.port = port
        self._nc = None
    
    async def connect(self) -> bool:
        try:
            import nats
            self._nc = await nats.connect(f"nats://{self.host}:{self.port}")
            self._connected = True
            self.set_concurrency(100)
            return True
        except Exception as e:
            print(f" [!] NATS async connection failed: {e}")
            return False
    
    async def disconnect(self):
        if self._nc:
            await self._nc.close()
        self._connected = False
    
    def _get_subject(self, target: int) -> str:
        return f"test.subject.{target}"
    
    async def _send_raw(self, envelope: MessageEnvelope) -> bool:
        try:
            subject = self._get_subject(envelope.target)
            data = envelope.serialize()
            await self._nc.publish(subject, data)
            return True
        except Exception:
            return False
    
    async def _send_with_ack(self, envelope: MessageEnvelope, timeout_ms: float) -> Optional[MessageEnvelope]:
        try:
            inbox = f"inbox_{envelope.message_id}"
            subject = self._get_subject(envelope.target)
            data = envelope.serialize()
            
            msg = await self._nc.request(subject, data, timeout=timeout_ms / 1000.0)
            if msg:
                return MessageEnvelope.deserialize(msg.data)
            return None
        except Exception:
            return None


class GrpcAsyncSender(UnifiedAsyncSender):
    """gRPC async sender implementation.
    
    Supports connecting to multiple receivers (one per port) for load balancing.
    Each receiver binds to port 50051 + receiver_id, so sender connects to those ports.
    """
    
    def __init__(self, base_port: int = 50051, num_receivers: int = 32):
        super().__init__("gRPC", "Python")
        self.base_port = base_port
        self.num_receivers = num_receivers
        self._channels = {}  # Map receiver_id -> channel
        self._stubs = {}     # Map receiver_id -> stub
        self._current_receiver = 0
    
    async def connect(self) -> bool:
        """Connect to all receiver ports for load balancing."""
        try:
            import grpc
            import messaging_pb2
            import messaging_pb2_grpc
            
            # Connect to each receiver port
            for receiver_id in range(self.num_receivers):
                port = self.base_port + receiver_id
                channel = grpc.aio.insecure_channel(f'localhost:{port}')
                stub = messaging_pb2_grpc.MessagingServiceStub(channel)
                self._channels[receiver_id] = channel
                self._stubs[receiver_id] = stub
            
            self._connected = True
            self.set_concurrency(100)
            print(f" [gRPC] Connected to {self.num_receivers} receivers on ports {self.base_port}-{self.base_port + self.num_receivers - 1}")
            return True
        except Exception as e:
            print(f" [!] gRPC async connection failed: {e}")
            return False
    
    async def disconnect(self):
        """Close all channels."""
        for channel in self._channels.values():
            await channel.close()
        self._channels.clear()
        self._stubs.clear()
        self._connected = False
    
    def _get_receiver_for_target(self, target: int) -> int:
        """Get the receiver ID to use for a given target."""
        # Round-robin load balancing across receivers
        return target % self.num_receivers
    
    async def _send_raw(self, envelope: MessageEnvelope) -> bool:
        try:
            import messaging_pb2
            receiver_id = self._get_receiver_for_target(envelope.target)
            stub = self._stubs.get(receiver_id)
            if stub:
                proto_env = envelope.to_protobuf()
                await stub.SendMessage(proto_env)
                return True
            return False
        except Exception:
            return False
    
    async def _send_with_ack(self, envelope: MessageEnvelope, timeout_ms: float) -> Optional[MessageEnvelope]:
        try:
            import messaging_pb2
            receiver_id = self._get_receiver_for_target(envelope.target)
            stub = self._stubs.get(receiver_id)
            if stub:
                proto_env = envelope.to_protobuf()
                response = await stub.SendMessage(
                    proto_env, 
                    timeout=timeout_ms / 1000.0
                )
                return MessageEnvelope.from_protobuf(response)
            return None
        except Exception:
            return None


class ActiveMQAsyncSender(UnifiedAsyncSender):
    """ActiveMQ async sender implementation."""
    
    def __init__(self, host: str = 'localhost', port: int = 61613):
        super().__init__("ActiveMQ", "Python")
        self.host = host
        self.port = port
        self._connection = None
    
    async def connect(self) -> bool:
        try:
            import stomp.asyncio
            self._conn = stomp.asyncio.AsyncConnection(
                [(self.host, self.port)]
            )
            await self._conn.connect('admin', 'admin', wait=True)
            self._connected = True
            self.set_concurrency(100)
            return True
        except Exception as e:
            print(f" [!] ActiveMQ async connection failed: {e}")
            return False
    
    async def disconnect(self):
        if self._connection:
            await self._connection.disconnect()
        self._connected = False
    
    def _get_destination(self, target: int) -> str:
        return f"/queue/test.queue.{target}"
    
    async def _send_raw(self, envelope: MessageEnvelope) -> bool:
        try:
            destination = self._get_destination(envelope.target)
            data = envelope.serialize()
            await self._conn.send(destination, data)
            return True
        except Exception:
            return False
    
    async def _send_with_ack(self, envelope: MessageEnvelope, timeout_ms: float) -> Optional[MessageEnvelope]:
        try:
            destination = self._get_destination(envelope.target)
            data = envelope.serialize()
            
            # Would need callback for real async ACK
            await self._conn.send(destination, data)
            return None
        except Exception:
            return None


def create_async_sender(service: str, **kwargs) -> UnifiedAsyncSender:
    """Factory function to create an async sender for the specified service."""
    senders = {
        'redis': RedisAsyncSender,
        'rabbitmq': RabbitMQAsyncSender,
        'zeromq': ZeroMQAsyncSender,
        'nats': NatsAsyncSender,
        'grpc': GrpcAsyncSender,
        'activemq': ActiveMQAsyncSender,
    }
    
    service_lower = service.lower()
    if service_lower not in senders:
        raise ValueError(f"Unknown service: {service}")
    
    return senders[service_lower](**kwargs)

