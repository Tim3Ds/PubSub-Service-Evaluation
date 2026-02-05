#!/usr/bin/env python3
"""RabbitMQ Python Receiver - Async"""
import sys
import signal
import asyncio
import aio_pika
from pathlib import Path

# Add utils to path
repo_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(repo_root / 'utils' / 'python'))

from message_helpers import *

running = True

def signal_handler(sig, frame):
    global running
    running = False

async def run(receiver_id):
    connection = await aio_pika.connect_robust("amqp://guest:guest@localhost/")
    
    async with connection:
        channel = await connection.channel()
        queue_name = f"test_queue_{receiver_id}"
        queue = await channel.declare_queue(queue_name)
        
        print(f" [*] [ASYNC] Receiver {receiver_id} waiting for messages on {queue_name}")
        
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    request_envelope = parse_envelope(message.body)
                    message_id = request_envelope.message_id
                    print(f" [x] [ASYNC] Received message {message_id}")
                    
                    # Create ACK
                    response = create_ack_from_envelope(request_envelope, str(receiver_id))
                    setattr(response, 'async', True)
                    resp_str = serialize_envelope(response)
                    
                    # Send reply
                    await channel.default_exchange.publish(
                        aio_pika.Message(
                            body=resp_str,
                            correlation_id=message.correlation_id,
                            content_type='application/octet-stream'
                        ),
                        routing_key=message.reply_to
                    )
                
                if not running:
                    break

    print(f" [x] [ASYNC] Receiver {receiver_id} shutting down")


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--id', type=int, default=0)
    args = parser.parse_args()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Loop setup
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        loop.run_until_complete(run(args.id))
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()


if __name__ == "__main__":
    main()
