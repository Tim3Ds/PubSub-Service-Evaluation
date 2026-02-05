#!/usr/bin/env python3
"""ZeroMQ Python Sender - Async"""
import sys
import json
import asyncio
import zmq
import zmq.asyncio
from pathlib import Path

# Add utils to path
repo_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(repo_root / 'utils' / 'python'))

from message_helpers import *
from test_data_loader import load_test_data
from stats_collector import MessageStats


async def send_message_task(context, item):
    """Send a single message asynchronously."""
    result = {'success': False, 'message_id': '', 'duration': 0, 'error': ''}
    
    socket = None
    try:
        message_id = extract_message_id(item)
        result['message_id'] = message_id
        target = item.get('target', 0)
        port = 5556 + target
        
        msg_start = get_current_time_ms()
        
        # Create new socket for each request (REQ/REP async pattern)
        socket = context.socket(zmq.REQ)
        socket.connect(f"tcp://localhost:{port}")
        
        # Create and send message
        envelope = create_data_envelope(item)
        body = serialize_envelope(envelope)
        
        await socket.send(body)
        
        # Wait for reply with timeout
        if await socket.poll(100): # 100ms
            response = await socket.recv()
            resp_envelope = parse_envelope(response)
            
            if is_valid_ack(resp_envelope, message_id):
                result['duration'] = get_current_time_ms() - msg_start
                result['success'] = True
            else:
                result['error'] = 'Invalid ACK'
        else:
            result['error'] = 'Timeout'
            
    except Exception as e:
        result['error'] = str(e)
    finally:
        if socket:
            socket.close()
    
    return result


async def run():
    test_data = load_test_data()
    
    stats = MessageStats()
    stats.set_metadata({
        'service': 'ZeroMQ',
        'language': 'Python',
        'async': True
    })
    start_time = get_current_time_ms()
    
    print(f" [x] Starting ASYNC transfer of {len(test_data)} messages...")
    
    context = zmq.asyncio.Context()
    
    tasks = [send_message_task(context, item) for item in test_data]
    results = await asyncio.gather(*tasks)
    
    for result in results:
        if result['success']:
            stats.record_message(True, result['duration'])
            print(f" [OK] Message {result['message_id']} acknowledged")
        else:
            stats.record_message(False)
            print(f" [FAILED] Message {result['message_id']}: {result['error']}")
    
    context.term()
    
    end_time = get_current_time_ms()
    stats.set_duration(start_time, end_time)
    
    report = stats.get_stats()
    
    print("\nTest Results (ASYNC):")
    print(f"total_sent: {stats.sent_count}")
    print(f"total_received: {stats.received_count}")
    print(f"duration_ms: {stats.get_duration_ms()}")
    
    with open('logs/report.txt', 'a') as f:
        f.write(json.dumps(report) + '\n')


def main():
    asyncio.run(run())


if __name__ == "__main__":
    main()
