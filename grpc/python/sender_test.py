#!/usr/bin/env python3
"""gRPC Python Sender - Sync"""
import sys
import json
import grpc
import time
from pathlib import Path

# Add utils to path
repo_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(repo_root / 'utils' / 'python'))

# Import generated protobuf code
import messaging_pb2
import messaging_pb2_grpc
from message_helpers import *
from test_data_loader import load_test_data
from stats_collector import MessageStats


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--base-port', type=int, default=50051)
    parser.add_argument('--num-receivers', type=int, default=1)
    args = parser.parse_args()
    
    test_data = load_test_data()
    
    stats = MessageStats()
    stats.set_metadata({
        'service': 'gRPC',
        'language': 'Python',
        'async': False
    })
    start_time = get_current_time_ms()
    
    print(f" [x] Starting transfer of {len(test_data)} messages...")
    
    # Cache channels
    channels = {}
    stubs = {}
    
    for item in test_data:
        message_id = extract_message_id(item)
        target = item.get('target', 0)
        port = args.base_port + target
        
        if target not in stubs:
             channel = grpc.insecure_channel(f'localhost:{port}')
             stub = messaging_pb2_grpc.MessagingServiceStub(channel)
             channels[target] = channel
             stubs[target] = stub
        
        stub = stubs[target]
        print(f" [x] Sending message {message_id} to target {target}...", end='', flush=True)
        msg_start = get_current_time_ms()
        
        # Create protobuf message directly/using helper
        envelope = create_data_envelope(item)
        
        try:
            # gRPC expects the protobuf object, not serialized bytes
            response = stub.SendMessage(envelope)
            
            if is_valid_ack(response, message_id):
                msg_duration = get_current_time_ms() - msg_start
                stats.record_message(True, msg_duration)
                print(" [OK]")
            else:
                stats.record_message(False)
                print(" [FAILED] Invalid ACK")
                
        except grpc.RpcError as e:
            stats.record_message(False)
            print(f" [FAILED] RPC Error: {e.code()}")
            
    # Cleanup
    for channel in channels.values():
        channel.close()
    
    end_time = get_current_time_ms()
    stats.set_duration(start_time, end_time)
    
    report = stats.get_stats()
    
    print("\nTest Results:")
    print(f"service: gRPC")
    print(f"language: Python")
    print(f"total_sent: {stats.sent_count}")
    print(f"total_received: {stats.received_count}")
    print(f"duration_ms: {stats.get_duration_ms()}")
    
    with open('logs/report.txt', 'a') as f:
        f.write(json.dumps(report) + '\n')


if __name__ == "__main__":
    main()
