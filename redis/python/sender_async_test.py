#!/usr/bin/env python3
"""
Redis Async Sender with targeted routing.
Uses redis-py async support for concurrent RPC calls.
"""
import asyncio
import redis.asyncio as redis
import json
import uuid
import sys
import os
import time

# Add harness to path for stats_collector
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../harness'))
from stats_collector import MessageStats, get_current_time_ms

class RedisAsyncSender:
    def __init__(self):
        self.r = redis.Redis(host='localhost', port=6379, db=0)
    async def call(self, message_data):
        target = message_data.get('target', 0)
        queue_name = f"test_queue_{target}"
        callback_queue = f"callback_{uuid.uuid4()}"
        
        message_data['reply_to'] = callback_queue
        await self.r.rpush(queue_name, json.dumps(message_data))
        
        # BLPOP with timeout for the response
        try:
            response = await self.r.blpop(callback_queue, timeout=0.2)
            if response:
                return response[1]
        except Exception as e:
            print(f" [!] Error during call: {e}")
        return None

async def send_message(sender, item, stats):
    target = item.get('target', 0)
    msg_start = get_current_time_ms()
    print(f" [x] [ASYNC] Sending message {item['message_id']} to target {target}...")
    
    response = await sender.call(item)
    
    if response:
        try:
            resp_data = json.loads(response)
            if resp_data.get('status') == 'ACK' and resp_data.get('message_id') == item['message_id']:
                msg_duration = get_current_time_ms() - msg_start
                stats.record_message(True, msg_duration)
                print(f" [OK] Message {item['message_id']} acknowledged")
            else:
                stats.record_message(False)
                print(f" [FAILED] Unexpected response for {item['message_id']}: {response}")
        except Exception as e:
            stats.record_message(False)
            print(f" [FAILED] Parse error for {item['message_id']}: {e}")
    else:
        stats.record_message(False)
        print(f" [FAILED] Timeout for {item['message_id']}")

async def main():
    sender = RedisAsyncSender()
    
    data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../test_data.json'))
    with open(data_path, 'r') as f:
        test_data = json.load(f)

    stats = MessageStats()
    stats.start_time = get_current_time_ms()

    print(f" [x] Starting transfer of {len(test_data)} messages (ASYNC)...")

    tasks = [send_message(sender, item, stats) for item in test_data]
    await asyncio.gather(*tasks)

    stats.end_time = get_current_time_ms()
    
    report = {
        "service": "Redis",
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

    await sender.r.aclose()

if __name__ == "__main__":
    asyncio.run(main())
