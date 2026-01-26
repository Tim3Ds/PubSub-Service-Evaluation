#!/usr/bin/env python3
"""
ZeroMQ Async Sender with P2P support.
Sends messages concurrently using asyncio.
"""
import zmq
import zmq.asyncio
import json
import sys
import os
import asyncio
import time

# Add harness to path for stats_collector
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../harness'))
from stats_collector import MessageStats, get_current_time_ms

async def send_message(ctx, sockets, locks, item, stats):
    target = item.get('target', 0)
    port = 5556 + target
    
    if target not in locks:
        locks[target] = asyncio.Lock()
    
    async with locks[target]:
        # Get or create socket for this target
        if target not in sockets:
            sock = ctx.socket(zmq.REQ)
            sock.connect(f"tcp://localhost:{port}")
            sock.setsockopt(zmq.RCVTIMEO, 500)  # 500ms timeout (async C++ receiver responsiveness)
            sockets[target] = sock
        
        socket = sockets[target]
        msg_start = get_current_time_ms()
        
        print(f" [x] [ASYNC] Sending message {item['message_id']} to target {target} (port {port})...")
        
        try:
            await socket.send_string(json.dumps(item))
            response = await socket.recv_string()
            resp_data = json.loads(response)
            
            if resp_data.get('status') == 'ACK' and resp_data.get('message_id') == item['message_id']:
                msg_duration = get_current_time_ms() - msg_start
                stats.record_message(True, msg_duration)
                print(f" [OK] Message {item['message_id']} acknowledged")
            else:
                stats.record_message(False)
                print(f" [FAILED] Unexpected response for {item['message_id']}: {response}")
        except zmq.error.Again:
            stats.record_message(False)
            print(f" [FAILED] Timeout for message {item['message_id']}")
            socket.close()
            if target in sockets: del sockets[target]
        except Exception as e:
            stats.record_message(False)
            print(f" [FAILED] Error for message {item['message_id']}: {e}")
            socket.close()
            if target in sockets: del sockets[target]

async def main():
    print(" [x] Starting ZeroMQ ASYNC Sender")
    ctx = zmq.asyncio.Context()
    sockets = {}
    locks = {}

    data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../test_data.json'))
    with open(data_path, 'r') as f:
        test_data = json.load(f)

    stats = MessageStats()
    stats.start_time = get_current_time_ms()

    print(f" [x] Starting async transfer of {len(test_data)} messages...")

    # We can use a semaphore to limit concurrency if desired, 
    # but for P2P we have up to 32 receivers.
    tasks = []
    for item in test_data:
        tasks.append(send_message(ctx, sockets, locks, item, stats))
    
    await asyncio.gather(*tasks)

    stats.end_time = get_current_time_ms()
    
    report = {
        "service": "ZeroMQ",
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

    # Cleanup
    for sock in sockets.values():
        sock.close()
    ctx.term()

if __name__ == "__main__":
    asyncio.run(main())
