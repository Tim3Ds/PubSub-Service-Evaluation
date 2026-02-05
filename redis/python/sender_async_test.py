#!/usr/bin/env python3
"""Redis Python Sender - Async"""
import sys
import json
import asyncio
import redis.asyncio as redis
from pathlib import Path

# Add utils to path
repo_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(repo_root / 'utils' / 'python'))

from message_helpers import *
from test_data_loader import load_test_data
from stats_collector import MessageStats


async def send_message_task(item):
    """Send a single message asynchronously with its own connection."""
    result = {'success': False, 'message_id': '', 'duration': 0, 'error': ''}
    
    # Each task gets its own Redis connection
    r = None
    pubsub = None
    
    try:
        message_id = extract_message_id(item)
        result['message_id'] = message_id
        target = item.get('target', 0)
        
        channel_name = f"test_channel_{target}"
        reply_channel = f"reply_channel_{message_id}"
        
        msg_start = get_current_time_ms()
        
        # Create dedicated connection for this task
        r = redis.Redis(host='localhost', port=6379, db=0)
        pubsub = r.pubsub()
        
        # Subscribe to reply channel
        await pubsub.subscribe(reply_channel)
        
        # Create and send message
        envelope = create_data_envelope(item)
        envelope.metadata['reply_to'] = reply_channel
        body = serialize_envelope(envelope)
        
        # Publish
        await r.publish(channel_name, body)
        
        # Wait for reply with timeout
        try:
            while True:
                message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=0.01)
                if message and message['type'] == 'message':
                    try:
                        resp_envelope = parse_envelope(message['data'])
                        if is_valid_ack(resp_envelope, message_id):
                            result['duration'] = get_current_time_ms() - msg_start
                            result['success'] = True
                            break
                        else:
                            # Ignore mismatched ACKs (usually late ACKs from previous messages)
                            pass
                    except Exception:
                        pass
                    
                # Timeout check (200ms - doubled for reliability)
                if (get_current_time_ms() - msg_start) > 200:
                    result['error'] = 'Timeout'
                    break
        except Exception as e:
            result['error'] = str(e)
            
    except Exception as e:
        result['error'] = str(e)
    finally:
        # Clean up resources
        if pubsub:
            try:
                await pubsub.unsubscribe(reply_channel)
                await pubsub.aclose()
            except:
                pass
        if r:
            try:
                await r.aclose()
            except:
                pass
    
    return result


async def run():
    test_data = load_test_data()
    
    stats = MessageStats()
    stats.set_metadata({
        'service': 'Redis',
        'language': 'Python',
        'async': True
    })
    start_time = get_current_time_ms()
    
    print(f" [x] Starting ASYNC transfer of {len(test_data)} messages...")
    
    # Give receivers time to subscribe (Redis pub/sub doesn't queue messages)
    await asyncio.sleep(0.5)
    
    # Process concurrently - each task gets its own connection
    tasks = [send_message_task(item) for item in test_data]
    results = await asyncio.gather(*tasks)
    
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
    asyncio.run(run())


if __name__ == "__main__":
    main()
