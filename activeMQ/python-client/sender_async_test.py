#!/usr/bin/env python3
"""ActiveMQ Python Sender - Async"""
import sys
import json
import asyncio
import time
import stomp
from pathlib import Path

# Add utils to path
repo_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(repo_root / 'utils' / 'python'))

from message_helpers import *
from test_data_loader import load_test_data
from stats_collector import MessageStats


class AsyncReplyListener(stomp.ConnectionListener):
    def __init__(self, loop):
        self.loop = loop
        self.futures = {} # Map correlation_id -> Future
        
    def on_message(self, frame):
        if 'correlation-id' in frame.headers:
            corr_id = frame.headers['correlation-id']
            if corr_id in self.futures:
                future = self.futures[corr_id]
                if not future.done():
                    # Handle body
                    body = frame.body
                    if isinstance(body, str):
                        body = body.encode('latin-1')  # latin-1 preserves bytes 0-255
                        
                    self.loop.call_soon_threadsafe(future.set_result, body)


async def send_message_task(conn, listener, item):
    """Send a single message asynchronously."""
    result = {'success': False, 'message_id': '', 'duration': 0, 'error': ''}
    
    try:
        message_id = extract_message_id(item)
        result['message_id'] = message_id
        target = item.get('target', 0)
        
        dest = f"/queue/test_queue_{target}"
        reply_dest = '/temp-queue/replies-async'
        
        msg_start = get_current_time_ms()
        
        # Create and send message
        envelope = create_data_envelope(item)
        body = serialize_envelope(envelope)
        
        corr_id = f"corr-async-{message_id}"
        
        # Create future for response
        future = asyncio.Future()
        listener.futures[corr_id] = future
        
        conn.send(body=body, destination=dest, headers={
            'reply-to': reply_dest,
            'correlation-id': corr_id,
            'content-type': 'application/octet-stream'
        })
        
        # Wait for reply with timeout
        try:
            resp_data = await asyncio.wait_for(future, timeout=0.1) # 100ms
            
            resp_envelope = parse_envelope(resp_data)
            if is_valid_ack(resp_envelope, message_id):
                result['duration'] = get_current_time_ms() - msg_start
                result['success'] = True
            else:
                result['error'] = 'Invalid ACK'
                
        except asyncio.TimeoutError:
            result['error'] = 'Timeout'
        finally:
            if corr_id in listener.futures:
                del listener.futures[corr_id]
            
    except Exception as e:
        result['error'] = str(e)
    
    return result


async def run():
    test_data = load_test_data()
    
    stats = MessageStats()
    stats.set_metadata({
        'service': 'ActiveMQ',
        'language': 'Python',
        'async': True
    })
    start_time = get_current_time_ms()
    
    print(f" [x] Starting ASYNC transfer of {len(test_data)} messages...")
    
    conn = stomp.Connection([('localhost', 61613)], auto_decode=False)
    loop = asyncio.get_event_loop()
    listener = AsyncReplyListener(loop)
    conn.set_listener('', listener)
    conn.connect('admin', 'admin', wait=True)
    
    reply_dest = '/temp-queue/replies-async'
    conn.subscribe(destination=reply_dest, id=1, ack='auto')
    
    # Process
    tasks = [send_message_task(conn, listener, item) for item in test_data]
    results = await asyncio.gather(*tasks)
    
    conn.disconnect()
    
    for result in results:
        if result['success']:
            stats.record_message(True, result['duration'])
            print(f" [OK] Message {result['message_id']} acknowledged")
        else:
            stats.record_message(False)
            print(f" [FAILED] Message {result['message_id']}: {result['error']}")
    
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
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(run())
    loop.close()


if __name__ == "__main__":
    main()
