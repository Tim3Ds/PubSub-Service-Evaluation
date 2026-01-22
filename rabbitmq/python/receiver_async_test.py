#!/usr/bin/env python3
"""
RabbitMQ Async Receiver.
Uses aio-pika to handle requests.
"""
import asyncio
import aio_pika
import json
import argparse
import signal
import sys

class RabbitMQAsyncReceiver:
    def __init__(self, receiver_id):
        self.receiver_id = receiver_id
        self.queue_name = f'test_queue_{receiver_id}'
        self.messages_received = 0
        self.connection = None

    async def run(self):
        self.connection = await aio_pika.connect_robust("amqp://guest:guest@localhost/")
        channel = await self.connection.channel()
        await channel.set_qos(prefetch_count=1)
        queue = await channel.declare_queue(self.queue_name)

        print(f" [*] [ASYNC] Receiver {self.receiver_id} awaiting messages on {self.queue_name}")

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    self.messages_received += 1
                    data = json.loads(message.body)
                    message_id = data.get('message_id')
                    print(f" [Receiver {self.receiver_id}] [ASYNC] Received message {message_id}")
                    
                    response = json.dumps({
                        "status": "ACK",
                        "message_id": message_id,
                        "receiver_id": self.receiver_id,
                        "async": True
                    })
                    
                    await channel.default_exchange.publish(
                        aio_pika.Message(
                            body=response.encode(),
                            correlation_id=message.correlation_id,
                        ),
                        routing_key=message.reply_to,
                    )

async def shutdown(receiver):
    print(f" [x] [ASYNC] Shutdown requested for RabbitMQ receiver {receiver.receiver_id} (received {receiver.messages_received} messages)")
    if receiver.connection:
        await receiver.connection.close()
    sys.exit(0)

async def main():
    parser = argparse.ArgumentParser(description='RabbitMQ Async Receiver')
    parser.add_argument('--id', type=int, default=0, help='Receiver ID (0-31)')
    args = parser.parse_args()
    
    receiver = RabbitMQAsyncReceiver(args.id)
    
    # Setup signal handlers
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown(receiver)))

    try:
        await receiver.run()
    except Exception as e:
        print(f" [!] [ASYNC] Receiver error: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
