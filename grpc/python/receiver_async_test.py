#!/usr/bin/env python3
"""
gRPC Async Receiver.
Uses grpc.aio to handle requests.
"""
import asyncio
import grpc
import test_data_pb2
import test_data_pb2_grpc
import argparse
import signal
import sys

class TestDataService(test_data_pb2_grpc.TestDataServiceServicer):
    def __init__(self, receiver_id):
        self.receiver_id = receiver_id
        self.messages_received = 0

    async def TransferData(self, request, context):
        self.messages_received += 1
        print(f" [Receiver {self.receiver_id}] [ASYNC] Received message {request.message_id}")
        return test_data_pb2.Ack(
            status='ACK',
            message_id=request.message_id,
            receiver_id=self.receiver_id
        )

async def serve(receiver_id):
    port = 50051 + receiver_id
    server = grpc.aio.server()
    test_data_pb2_grpc.add_TestDataServiceServicer_to_server(
        TestDataService(receiver_id), server)
    server.add_insecure_port(f'[::]:{port}')
    print(f" [*] [ASYNC] Receiver {receiver_id} started on port {port}")
    await server.start()
    
    # Store for shutdown
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(server.stop(5)))
        
    await server.wait_for_termination()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='gRPC Async Receiver')
    parser.add_argument('--id', type=int, default=0, help='Receiver ID (0-31)')
    args = parser.parse_args()
    
    try:
        asyncio.run(serve(args.id))
    except KeyboardInterrupt:
        pass
