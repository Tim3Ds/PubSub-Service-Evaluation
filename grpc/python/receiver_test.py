#!/usr/bin/env python3
"""gRPC Python Receiver - Sync"""
import sys
import signal
import grpc
import time
from concurrent import futures
from pathlib import Path

# Add utils to path
repo_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(repo_root / 'utils' / 'python'))

import messaging_pb2
import messaging_pb2_grpc
from message_helpers import *

running = True

def signal_handler(sig, frame):
    global running
    running = False

class MessagingServicer(messaging_pb2_grpc.MessagingServiceServicer):
    def __init__(self, receiver_id):
        self.receiver_id = receiver_id
        
    def SendMessage(self, request, context):
        message_id = request.message_id
        print(f" [x] Received message {message_id}")
        
        # Create ACK using helper
        # Helper returns MessageEnvelope object
        response = create_ack_from_envelope(request, str(self.receiver_id))
        return response

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--id', type=int, default=0)
    parser.add_argument('--server-port', type=int, default=50051)
    args = parser.parse_args()
    
    receiver_id = args.id
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    port = args.server_port + receiver_id
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    messaging_pb2_grpc.add_MessagingServiceServicer_to_server(
        MessagingServicer(receiver_id), server
    )
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    
    print(f" [*] Receiver {receiver_id} listening on port {port}")
    
    while running:
        time.sleep(0.1)
        
    print(f" [x] Receiver {receiver_id} shutting down")
    server.stop(0)


if __name__ == "__main__":
    main()
