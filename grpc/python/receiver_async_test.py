#!/usr/bin/env python3
"""gRPC Python Receiver - Async"""
import sys
import signal
import asyncio
import grpc
from pathlib import Path

# Add utils to path
repo_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(repo_root / 'utils' / 'python'))

import messaging_pb2
import messaging_pb2_grpc
from message_helpers import *

class AsyncMessagingServicer(messaging_pb2_grpc.MessagingServiceServicer):
    def __init__(self, receiver_id):
        self.receiver_id = receiver_id
        
    async def SendMessage(self, request, context):
        message_id = request.message_id
        print(f" [x] [ASYNC] Received message {message_id}")
        
        # Create ACK
        response = create_ack_from_envelope(request, str(self.receiver_id))
        setattr(response, 'async', True)
        return response

async def run(receiver_id, server_port):
    server = grpc.aio.server()
    messaging_pb2_grpc.add_MessagingServiceServicer_to_server(
        AsyncMessagingServicer(receiver_id), server
    )
    port = server_port + receiver_id
    server.add_insecure_port(f'[::]:{port}')
    
    print(f" [*] [ASYNC] Receiver {receiver_id} listening on port {port}")
    await server.start()
    
    try:
        await server.wait_for_termination()
    except asyncio.CancelledError:
        await server.stop(0)

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--id', type=int, default=0)
    parser.add_argument('--server-port', type=int, default=50051)
    args = parser.parse_args()
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    task = loop.create_task(run(args.id, args.server_port))
    
    def signal_handler():
        task.cancel()
        
    loop.add_signal_handler(signal.SIGINT, signal_handler)
    loop.add_signal_handler(signal.SIGTERM, signal_handler)
    
    try:
        loop.run_until_complete(task)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        loop.close()


if __name__ == "__main__":
    main()
