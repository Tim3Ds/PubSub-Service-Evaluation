"""
gRPC Receiver with targeted routing support for multi-receiver testing.
Each receiver listens on port 50051 + receiver_id.
Usage: python receiver_test.py --id <0-31>
"""
import grpc
import test_data_pb2
import test_data_pb2_grpc
from concurrent import futures
import time
import argparse
import logging
import sys

# Set up logging
logging.basicConfig(
    format='[Receiver %(receiver_id)s] %(levelname)s: %(message)s',
    level=logging.INFO,
    stream=sys.stdout
)

class TestDataServiceServicer(test_data_pb2_grpc.TestDataServiceServicer):
    def __init__(self, receiver_id):
        self.receiver_id = receiver_id
        self.logger = logging.LoggerAdapter(
            logging.getLogger(),
            {'receiver_id': receiver_id}
        )
        self.messages_received = 0
        
    def TransferData(self, request, context):
        self.messages_received += 1
        self.logger.info(f"Received message {request.message_id}")
        return test_data_pb2.Ack(
            message_id=request.message_id,
            status='ACK',
            receiver_id=self.receiver_id
        )

def serve():
    parser = argparse.ArgumentParser(description='gRPC Receiver')
    parser.add_argument('--id', type=int, default=0, help='Receiver ID (0-31)')
    args = parser.parse_args()
    
    receiver_id = args.id
    port = 50051 + receiver_id
    
    servicer = TestDataServiceServicer(receiver_id)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    test_data_pb2_grpc.add_TestDataServiceServicer_to_server(servicer, server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    print(f" [*] Receiver {receiver_id} gRPC Server started on port {port}")
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        print(f" [x] Receiver {receiver_id} shutting down (received {servicer.messages_received} messages)")
        server.stop(0)

if __name__ == "__main__":
    serve()
