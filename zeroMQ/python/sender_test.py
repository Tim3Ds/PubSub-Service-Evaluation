#!/usr/bin/env python3
"""
ZeroMQ Sender with P2P support for multi-receiver testing.
Connects directly to each receiver's port (5556 + target) using REQ sockets.
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
    
    # Cache of sockets per target
    sockets = {}

    data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../test_data.json'))
    with open(data_path, 'r') as f:
        test_data = json.load(f)

    stats = MessageStats()
    stats.start_time = get_current_time_ms()

    print(f" [x] Starting transfer of {len(test_data)} messages...")

    for item in test_data:
        target = item.get('target', 0)
        port = 5556 + target
        
        # Get or create socket for this target
        if target not in sockets:
            sock = ctx.socket(zmq.REQ)
            sock.connect(f"tcp://localhost:{port}")
            sock.setsockopt(zmq.RCVTIMEO, 500)  # 500ms timeout (async C++ receiver responsiveness)
            sockets[target] = sock
        
        socket = sockets[target]
        msg_start = get_current_time_ms()
        
        print(f" [x] Sending message {item['message_id']} to target {target} (port {port})...", end='', flush=True)
        
        try:
            socket.send_string(json.dumps(item))
            response = socket.recv_string()
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
            # Close poisoned REQ socket
            socket.close()
            del sockets[target]
        except Exception as e:
            stats.record_message(False)
            print(f" [FAILED] Error: {e}")
            # Close poisoned REQ socket on any error to be safe
            socket.close()
            del sockets[target]

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

    # Cleanup
    for sock in sockets.values():
        sock.close()
    ctx.term()

if __name__ == "__main__":
    main()
