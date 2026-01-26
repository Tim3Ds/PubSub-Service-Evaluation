#!/usr/bin/env python3
"""
NATS Async Sender with targeted routing.
Sends messages concurrently using asyncio.gather.
"""
import asyncio
from nats.aio.client import Client as NATS
import json
import sys
import os
import time

# Add harness to path for stats_collector
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../harness'))
from stats_collector import MessageStats, get_current_time_ms

async def send_message(nc, item, stats):
    target = item.get('target', 0)
    subject = f"test.receiver.{target}"
    msg_start = get_current_time_ms()
    
    print(f" [x] [ASYNC] Sending message {item['message_id']} to target {target}...")
    try:
        response = await nc.request(subject, json.dumps(item).encode(), timeout=0.2)
        resp_data = json.loads(response.data.decode())
        
        if resp_data.get('status') == 'ACK' and resp_data.get('message_id') == item['message_id']:
            msg_duration = get_current_time_ms() - msg_start
            stats.record_message(True, msg_duration)
            print(f" [OK] Message {item['message_id']} acknowledged")
        else:
            stats.record_message(False)
            print(f" [FAILED] Unexpected response for {item['message_id']}: {response.data.decode()}")
    except Exception as e:
        stats.record_message(False)
        print(f" [FAILED] Error for message {item['message_id']}: {e}")

async def main():
    nc = NATS()
    await nc.connect(servers=["nats://localhost:4222"])

    data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../test_data.json'))
    with open(data_path, 'r') as f:
        test_data = json.load(f)

    stats = MessageStats()
    stats.start_time = get_current_time_ms()

    print(f" [x] Starting transfer of {len(test_data)} messages (ASYNC)...")

    tasks = [send_message(nc, item, stats) for item in test_data]
    await asyncio.gather(*tasks)

    stats.end_time = get_current_time_ms()
    
    report = {
        "service": "NATS",
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

    await nc.close()

if __name__ == "__main__":
    asyncio.run(main())
