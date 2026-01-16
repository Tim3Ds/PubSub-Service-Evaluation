#!/usr/bin/env python3
"""
ZeroMQ Sender with targeted routing support for multi-receiver testing.
Uses DEALER socket to route messages to specific receivers via a ROUTER.
"""
import zmq
import json
import sys
import os
import time

# Add harness to path for stats_collector
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../harness'))
from stats_collector import MessageStats, get_current_time_ms

def main():
    ctx = zmq.Context()
    socket = ctx.socket(zmq.DEALER)
    socket.setsockopt_string(zmq.IDENTITY, "sender")
    socket.connect("tcp://localhost:5555")

    data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../test_data.json'))
    with open(data_path, 'r') as f:
        test_data = json.load(f)

    stats = MessageStats()
    stats.start_time = get_current_time_ms()

    print(f" [x] Starting transfer of {len(test_data)} messages...")

    for item in test_data:
        target = item.get('target', 0)
        receiver_id = f"receiver_{target}"
        msg_start = get_current_time_ms()
        
        print(f" [x] Sending message {item['message_id']} to target {target}...", end='', flush=True)
        
        try:
            # Send to specific receiver via ROUTER
            # ROUTER expects [identity, empty?, data] or just [identity, data]?
            # DEALER/ROUTER: [identity, data]
            socket.send_multipart([
                receiver_id.encode(),
                json.dumps(item).encode()
            ])
            
            # Wait for response
            socket.setsockopt(zmq.RCVTIMEO, 5000)
            # ROUTER receives [identity, data]
            frames = socket.recv_multipart()
            if len(frames) >= 2:
                response = frames[1].decode()
            else:
                response = frames[0].decode() # Should not happen with valid Router/Dealer logic

            resp_data = json.loads(response)
            
            if resp_data.get('status') == 'ACK' and resp_data.get('message_id') == item['message_id']:
                msg_duration = get_current_time_ms() - msg_start
                stats.record_message(True, msg_duration)
                print(" [OK]")
            else:
                stats.record_message(False)
                print(f" [FAILED] Unexpected response: {response}")
        except zmq.error.Again:
            stats.record_message(False)
            print(" [FAILED] Timeout")
        except Exception as e:
            stats.record_message(False)
            print(f" [FAILED] Error: {e}")

    stats.end_time = get_current_time_ms()
    
    report = {
        "service": "ZeroMQ",
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

    socket.close()
    ctx.term()

if __name__ == "__main__":
    main()
