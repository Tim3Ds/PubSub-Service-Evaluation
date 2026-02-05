#!/usr/bin/env python3
"""gRPC Python Sender - Async"""
import sys
import json
import asyncio
import grpc
from pathlib import Path

# Add utils to path
repo_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(repo_root / 'utils' / 'python'))

import messaging_pb2
import messaging_pb2_grpc
from message_helpers import *
from test_data_loader import load_test_data
from stats_collector import MessageStats


async def send_message_task(stubs, item):
    """Send a single message asynchronously."""
    result = {'success': False, 'message_id': '', 'duration': 0, 'error': ''}
    
    try:
        message_id = extract_message_id(item)
        result['message_id'] = message_id
        target = item.get('target', 0)
        
        stub = stubs.get(target)
        if not stub:
             # Should create channel dynamically but for brevity assuming pre-created
             # In real async app, would manage pool
             result['error'] = 'No connection to target'
             return result
             
        msg_start = get_current_time_ms()
        
        envelope = create_data_envelope(item)
        
        response = await stub.SendMessage(envelope)
        
        if is_valid_ack(response, message_id):
            result['duration'] = get_current_time_ms() - msg_start
            result['success'] = True
        else:
            result['error'] = 'Invalid ACK'
            
    except grpc.RpcError as e:
        result['error'] = f"RPC Error: {e.code()}"
    except Exception as e:
        result['error'] = str(e)
    
    return result


async def run(base_port, num_receivers):
    test_data = load_test_data()
    
    stats = MessageStats()
    stats.set_metadata({
        'service': 'gRPC',
        'language': 'Python',
        'async': True
    })
    start_time = get_current_time_ms()
    
    print(f" [x] Starting ASYNC transfer of {len(test_data)} messages...")
    
    # Pre-create channels/stubs
    channels = []
    stubs = {}
    targets = set(item.get('target', 0) for item in test_data)
    
    for target in targets:
        port = base_port + target
        channel = grpc.aio.insecure_channel(f'localhost:{port}')
        stub = messaging_pb2_grpc.MessagingServiceStub(channel)
        channels.append(channel)
        stubs[target] = stub
    
    tasks = [send_message_task(stubs, item) for item in test_data]
    results = await asyncio.gather(*tasks)
    
    # Cleanup
    for channel in channels:
        await channel.close()
    
    for result in results:
        if result['success']:
            stats.record_message(True, result['duration'])
            print(f" [OK] Message {result['message_id']} acknowledged")
        else:
            stats.record_message(False)
            print(f" [FAILED] Message {result['message_id']}: {result['error']}")
    
    end_time = get_current_time_ms()
    stats.set_duration(start_time, end_time)
    
    report = stats.get_stats()
    
    print("\nTest Results (ASYNC):")
    print(f"total_sent: {stats.sent_count}")
    print(f"total_received: {stats.received_count}")
    print(f"duration_ms: {stats.get_duration_ms()}")
    
    with open('logs/report.txt', 'a') as f:
        f.write(json.dumps(report) + '\n')


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--base-port', type=int, default=50051)
    parser.add_argument('--num-receivers', type=int, default=1)
    args = parser.parse_args()
    
    asyncio.run(run(args.base_port, args.num_receivers))


if __name__ == "__main__":
    main()
