#!/usr/bin/env python3
"""
gRPC Async Sender with targeted routing.
Uses grpc.aio for concurrent RPC calls.
"""
import asyncio
import grpc
import test_data_pb2
import test_data_pb2_grpc
import json
import sys
import os
import time

# Add harness to path for stats_collector
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../harness'))
from stats_collector import MessageStats, get_current_time_ms

async def send_message(stubs, item, stats):
    target = item.get('target', 0)
    message_id = int(item['message_id'])
    msg_start = get_current_time_ms()
    print(f" [x] [ASYNC] Sending message {message_id} to target {target}...")
    try:
        request = test_data_pb2.TestDataItem(
            message_id=message_id,
            message_name=item['message_name'],
            message_value=item['message_value'],
            target=target
        )
        response = await stubs[target].TransferData(request, timeout=0.04)
        
        if response.status == 'ACK' and response.message_id == message_id:
            msg_duration = get_current_time_ms() - msg_start
            stats.record_message(True, msg_duration)
            print(f" [OK] Message {item['message_id']} acknowledged")
        else:
            stats.record_message(False)
            print(f" [FAILED] Unexpected response for {item['message_id']}: {response.status}")
    except Exception as e:
        stats.record_message(False)
        print(f" [FAILED] Error for {item['message_id']}: {e}")

async def main():
    data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../test_data.json'))
    with open(data_path, 'r') as f:
        test_data = json.load(f)

    # Create channels for all 32 receivers
    channels = {}
    stubs = {}
    for i in range(32):
        port = 50051 + i
        channels[i] = grpc.aio.insecure_channel(f'localhost:{port}')
        stubs[i] = test_data_pb2_grpc.TestDataServiceStub(channels[i])

    stats = MessageStats()
    stats.start_time = get_current_time_ms()

    print(f" [x] Starting transfer of {len(test_data)} messages (ASYNC)...")

    tasks = [send_message(stubs, item, stats) for item in test_data]
    await asyncio.gather(*tasks)

    stats.end_time = get_current_time_ms()
    
    report = {
        "service": "gRPC",
        "language": "Python",
        "async": True,
        **stats.get_stats()
    }

    print("\nTest Results (ASYNC):")
    for k, v in report.items():
        if k != 'message_timing_stats':
            print(f"{k}: {v}")

    report_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../report.txt'))
    with open(report_path, 'a') as f:
        f.write(json.dumps(report) + "\n")

    # Close channels
    for ch in channels.values():
        await ch.close()

if __name__ == "__main__":
    asyncio.run(main())
