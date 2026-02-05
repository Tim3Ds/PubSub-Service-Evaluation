#!/usr/bin/env python3
"""Redis Python Sender - Sync"""
import sys
import json
import time
import redis
from pathlib import Path

# Add utils to path
repo_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(repo_root / 'utils' / 'python'))

from message_helpers import *
from test_data_loader import load_test_data
from stats_collector import MessageStats


def main():
    test_data = load_test_data()
    
    stats = MessageStats()
    stats.set_metadata({
        'service': 'Redis',
        'language': 'Python',
        'async': False
    })
    start_time = get_current_time_ms()
    
    print(f" [x] Starting transfer of {len(test_data)} messages...")
    
    r = redis.Redis(host='localhost', port=6379, db=0)
    pubsub = r.pubsub(ignore_subscribe_messages=True)
    
    for item in test_data:
        message_id = extract_message_id(item)
        target = item.get('target', 0)
        print(f" [x] Sending message {message_id} to target {target}...", end='', flush=True)
        
        channel_name = f"test_channel_{target}"
        reply_channel = f"reply_channel_{message_id}"
        
        msg_start = get_current_time_ms()
        
        # Subscribe to reply channel
        pubsub.subscribe(reply_channel)
        
        # Create and send protobuf message
        envelope = create_data_envelope(item)
        envelope.metadata['reply_to'] = reply_channel
        body = serialize_envelope(envelope)
        
        # Publish with retry (handle potential race condition)
        receivers = 0
        for _ in range(3):
            receivers = r.publish(channel_name, body)
            if receivers > 0:
                break
            time.sleep(0.01)
        
        print(f" (reached {receivers} receivers)...", end='', flush=True)
            
        # Wait for reply
        response_received = False
        start_wait = time.time()
        while (time.time() - start_wait) < 0.08:  # 80ms timeout
            message = pubsub.get_message(timeout=0.01)
            if message and message['type'] == 'message':
                try:
                    resp_envelope = parse_envelope(message['data'])
                    if is_valid_ack(resp_envelope, message_id):
                        msg_duration = get_current_time_ms() - msg_start
                        stats.record_message(True, msg_duration)
                        print(" [OK]")
                        response_received = True
                        break
                    else:
                        # Ignore mismatched ACKs (usually late ACKs from previous messages)
                        continue
                except Exception:
                    continue
        
        if not response_received:
            stats.record_message(False)
            print(" [FAILED] Timeout")
            
        pubsub.unsubscribe(reply_channel)
        
    pubsub.close()
    r.close()
    
    end_time = get_current_time_ms()
    stats.set_duration(start_time, end_time)
    
    report = stats.get_stats()
    
    print("\nTest Results:")
    print(f"service: Redis")
    print(f"language: Python")
    print(f"total_sent: {stats.sent_count}")
    print(f"total_received: {stats.received_count}")
    print(f"duration_ms: {stats.get_duration_ms()}")
    
    with open('logs/report.txt', 'a') as f:
        f.write(json.dumps(report) + '\n')


if __name__ == "__main__":
    main()
