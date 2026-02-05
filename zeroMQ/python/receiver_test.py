#!/usr/bin/env python3
"""ZeroMQ Python Receiver - Sync"""
import sys
import signal
import zmq
from pathlib import Path

# Add utils to path
repo_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(repo_root / 'utils' / 'python'))

from message_helpers import *

running = True

def signal_handler(sig, frame):
    global running
    running = False

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--id', type=int, default=0)
    args = parser.parse_args()
    
    receiver_id = args.id
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    port = 5556 + receiver_id
    socket.bind(f"tcp://*:{port}")
    
    print(f" [*] Receiver {receiver_id} listening on port {port}")
    
    while running:
        try:
            # Non-blocking check or poll would be better but simple blocking with signal works on py3
            # Using poller to allow graceful shutdown
            if socket.poll(100):
                message = socket.recv()
                
                request_envelope = parse_envelope(message)
                message_id = request_envelope.message_id
                print(f" [x] Received message {message_id}")
                
                # Create ACK
                response = create_ack_from_envelope(request_envelope, str(receiver_id))
                resp_str = serialize_envelope(response)
                
                socket.send(resp_str)
        except zmq.ZMQError as e:
            if running:
                print(f"Error: {e}")
                
    print(f" [x] Receiver {receiver_id} shutting down")
    socket.close()
    context.term()


if __name__ == "__main__":
    main()
