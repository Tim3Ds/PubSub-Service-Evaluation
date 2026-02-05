#!/usr/bin/env python3
"""Redis Python Receiver - Async"""
import sys
import signal
import asyncio
import redis.asyncio as redis
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
    r = redis.Redis(host='localhost', port=6379, db=0)
    pubsub = r.pubsub(ignore_subscribe_messages=True)
    
    channel_name = f"test_channel_{receiver_id}"
    await pubsub.subscribe(channel_name)
    
    print(f" [*] [ASYNC] Receiver {receiver_id} waiting for messages on {channel_name}")
    
    while running:
        try:
            message = await pubsub.get_message(timeout=0.1)
            if message and message['type'] == 'message':
                request_envelope = parse_envelope(message['data'])
                message_id = request_envelope.message_id
                print(f" [x] [ASYNC] Received message {message_id}")
                
                # Create ACK
                response = create_ack_from_envelope(request_envelope, str(receiver_id))
                setattr(response, 'async', True)
                resp_str = serialize_envelope(response)
                
                # Send reply
                if 'reply_to' in request_envelope.metadata:
                    await r.publish(request_envelope.metadata['reply_to'], resp_str)
        except Exception as e:
            print(f"Error: {e}")
            await asyncio.sleep(0.1)
            
    print(f" [x] [ASYNC] Receiver {receiver_id} shutting down")
    await pubsub.close()
    await r.close()


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--id', type=int, default=0)
    args = parser.parse_args()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    asyncio.run(run(args.id))


if __name__ == "__main__":
    main()
