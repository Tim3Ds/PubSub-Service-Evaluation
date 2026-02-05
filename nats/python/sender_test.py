#!/usr/bin/env python3
"""NATS Python Sender - Sync"""
import sys
import json
import nats
import asyncio
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
        'service': 'NATS',
        'language': 'Python',
        'async': False
    })
    start_time = get_current_time_ms()
    
    print(f" [x] Starting transfer of {len(test_data)} messages...")
    
    async def run():
        nc = await nats.connect("nats://localhost:4222")
        
        for item in test_data:
            message_id = extract_message_id(item)
            target = item.get('target', 0)
            print(f" [x] Sending message {message_id} to target {target}...", end='', flush=True)
            
            subject = f"test.subject.{target}"
            msg_start = get_current_time_ms()
            
            # Create and send protobuf message
            envelope = create_data_envelope(item)
            body = serialize_envelope(envelope)
            
            try:
                response = await nc.request(subject, body, timeout=0.04)  # 40ms
                
                # Parse and validate ACK
                resp_envelope = parse_envelope(response.data)
                if is_valid_ack(resp_envelope, message_id):
                    msg_duration = get_current_time_ms() - msg_start
                    stats.record_message(True, msg_duration)
                    print(" [OK]")
                else:
                    stats.record_message(False)
                    print(" [FAILED] Invalid ACK")
            except asyncio.TimeoutError:
                stats.record_message(False)
                print(" [FAILED] Timeout")
            except Exception as e:
                stats.record_message(False)
                print(f" [FAILED] {e}")
        
        await nc.close()
    
    asyncio.run(run())
    
    end_time = get_current_time_ms()
    stats.set_duration(start_time, end_time)
    
    report = stats.get_stats()
    
    print("\nTest Results:")
    print(f"service: NATS")
    print(f"language: Python")
    print(f"total_sent: {stats.sent_count}")
    print(f"total_received: {stats.received_count}")
    print(f"duration_ms: {stats.get_duration_ms()}")
    
    with open('logs/report.txt', 'a') as f:
        f.write(json.dumps(report) + '\n')


if __name__ == "__main__":
    main()
