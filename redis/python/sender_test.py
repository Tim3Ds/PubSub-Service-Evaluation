#!/usr/bin/env python3
"""
Redis Sender with targeted routing support for multi-receiver testing.
Routes each message to test_queue_{target} based on the target field.
"""
import redis
import json
import uuid
import sys
import os
import time
import argparse

# Add harness to path for stats_collector
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../harness'))
from stats_collector import MessageStats, get_current_time_ms

class RedisSender:
    def __init__(self):
        self.r = redis.Redis(host='localhost', port=6379, db=0)
        self.callback_queue = f"callback_{uuid.uuid4()}"

    def call(self, message_data):
        target = message_data.get('target', 0)
        queue_name = f"test_queue_{target}"
        
        message_data['reply_to'] = self.callback_queue
        self.r.rpush(queue_name, json.dumps(message_data))
        
        # BLPOP with timeout for the response
        response = self.r.blpop(self.callback_queue, timeout=5)
        if response:
            return response[1]
        return None

def main():
    sender = RedisSender()
    
    data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../test_data.json'))
    with open(data_path, 'r') as f:
        test_data = json.load(f)

    stats = MessageStats()
    stats.start_time = get_current_time_ms()

    print(f" [x] Starting transfer of {len(test_data)} messages...")

    for item in test_data:
        target = item.get('target', 0)
        msg_start = get_current_time_ms()
        print(f" [x] Sending message {item['message_id']} to target {target}...", end='', flush=True)
        
        response = sender.call(item)
        
        if response:
            try:
                resp_data = json.loads(response)
                if resp_data.get('status') == 'ACK' and resp_data.get('message_id') == item['message_id']:
                    msg_duration = get_current_time_ms() - msg_start
                    stats.record_message(True, msg_duration)
                    print(" [OK]")
                else:
                    stats.record_message(False)
                    print(f" [FAILED] Unexpected response: {response}")
            except Exception as e:
                stats.record_message(False)
                print(f" [FAILED] Parse error: {e}")
        else:
            stats.record_message(False)
            print(" [FAILED] Timeout")

    stats.end_time = get_current_time_ms()
    
    report = {
        "service": "Redis",
        "language": "Python",
        **stats.get_stats()
    }

    print("\nTest Results:")
    for k, v in report.items():
        if k != 'message_timing_stats':
            print(f"{k}: {v}")

    report_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../report.txt'))
    with open(report_path, 'a') as f:
        f.write(json.dumps(report) + "\n")

if __name__ == "__main__":
    main()
