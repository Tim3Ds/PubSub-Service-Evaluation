#!/usr/bin/env python3
"""NATS Python Receiver - Async"""
import sys
import signal
import nats
import asyncio
from pathlib import Path

# Add utils to path
repo_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(repo_root / 'utils' / 'python'))

from message_helpers import *

running = True

def signal_handler(sig, frame):
    global running
    running = False


async def message_handler(msg, receiver_id):
    """Handle incoming message asynchronously."""
    request_str = msg.data
    
    # Parse message
    request_envelope = parse_envelope(request_str)
    message_id = request_envelope.message_id
    print(f" [x] [ASYNC] Received message {message_id}")
    
    # Create ACK
    response = create_ack_from_envelope(request_envelope, str(receiver_id))
    setattr(response, 'async', True)
    resp_str = serialize_envelope(response)
    
    # Send reply
    await msg.respond(resp_str)


async def run(receiver_id):
    nc = await nats.connect("nats://localhost:4222")
    
    subject = f"test.subject.{receiver_id}"
    
    async def cb(msg):
        await message_handler(msg, receiver_id)

    # Subscribe with handler
    await nc.subscribe(subject, cb=cb)
    
    print(f" [*] [ASYNC] Receiver {receiver_id} subscribed to {subject}")
    
    # Keep running
    while running:
        await asyncio.sleep(0.1)
    
    print(f" [x] [ASYNC] Receiver {receiver_id} shutting down")
    await nc.close()


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
