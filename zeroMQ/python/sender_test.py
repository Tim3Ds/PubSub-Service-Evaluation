#!/usr/bin/env python3
"""ZeroMQ Python Sender - Sync"""
import sys
import json
import zmq
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
        'service': 'ZeroMQ',
        'language': 'Python',
        'async': False
    })
    start_time = get_current_time_ms()
    
    print(f" [x] Starting transfer of {len(test_data)} messages...")
    
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    
    # Configure socket
    socket.setsockopt(zmq.RCVTIMEO, 100) # 100ms timeout
    socket.setsockopt(zmq.LINGER, 0)
    
    current_target = -1
    
    for item in test_data:
        message_id = extract_message_id(item)
        target = item.get('target', 0)
        
        # Connect to correct target port if changed
        if target != current_target:
             # Disconnect previous if needed? ZMQ handles multi-connect but REQ expects 1 reply per request
             # For simplicity, we can recreate socket or connect to all
             # But strictly speaking, REQ socket connecting to multiple endpoints does load balancing
             # To target specific receiver, we need specific connection
             
             socket.close()
             socket = context.socket(zmq.REQ)
             socket.setsockopt(zmq.RCVTIMEO, 100)
             socket.connect(f"tcp://localhost:{5556 + target}")
             current_target = target
             
        print(f" [x] Sending message {message_id} to target {target}...", end='', flush=True)
        msg_start = get_current_time_ms()
        
        # Create and send protobuf message
        envelope = create_data_envelope(item)
        body = serialize_envelope(envelope)
        
        socket.send(body)
        
        try:
            response = socket.recv()
            resp_envelope = parse_envelope(response)
            
            if is_valid_ack(resp_envelope, message_id):
                msg_duration = get_current_time_ms() - msg_start
                stats.record_message(True, msg_duration)
                print(" [OK]")
            else:
                stats.record_message(False)
                print(" [FAILED] Invalid ACK")
                
        except zmq.Again:
            stats.record_message(False)
            print(" [FAILED] Timeout")
            # Recreate socket on timeout to clear state
            socket.close()
            socket = context.socket(zmq.REQ)
            socket.setsockopt(zmq.RCVTIMEO, 100)
            socket.connect(f"tcp://localhost:{5556 + target}")
            
        except Exception as e:
            stats.record_message(False)
            print(f" [FAILED] {e}")
            
    socket.close()
    context.term()
    
    end_time = get_current_time_ms()
    stats.set_duration(start_time, end_time)
    
    report = stats.get_stats()
    
    print("\nTest Results:")
    print(f"service: ZeroMQ")
    print(f"language: Python")
    print(f"total_sent: {stats.sent_count}")
    print(f"total_received: {stats.received_count}")
    print(f"duration_ms: {stats.get_duration_ms()}")
    
    with open('logs/report.txt', 'a') as f:
        f.write(json.dumps(report) + '\n')


if __name__ == "__main__":
    main()
