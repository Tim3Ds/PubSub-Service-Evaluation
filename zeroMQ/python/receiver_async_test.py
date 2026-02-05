#!/usr/bin/env python3
"""ZeroMQ Python Receiver - Async"""
import sys
import signal
import asyncio
import zmq
import zmq.asyncio
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
    context = zmq.asyncio.Context()
    socket = context.socket(zmq.REP)
    port = 5556 + receiver_id
    socket.bind(f"tcp://*:{port}")
    
    print(f" [*] [ASYNC] Receiver {receiver_id} listening on port {port}")
    
    while running:
        if await socket.poll(100):
            message = await socket.recv()
            
            request_envelope = parse_envelope(message)
            message_id = request_envelope.message_id
            print(f" [x] [ASYNC] Received message {message_id}")
            
            # Create ACK
            response = create_ack_from_envelope(request_envelope, str(receiver_id))
            setattr(response, 'async', True)
            resp_str = serialize_envelope(response)
            
            await socket.send(resp_str)
        else:
            # Yield control to allow loop to process signals/other tasks
            await asyncio.sleep(0.01)
            
    print(f" [x] [ASYNC] Receiver {receiver_id} shutting down")
    socket.close()
    context.term()


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
