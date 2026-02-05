#!/usr/bin/env python3
"""RabbitMQ Python Sender - Async"""
import sys
import json
import asyncio
import aio_pika
from pathlib import Path

# Add utils to path
repo_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(repo_root / 'utils' / 'python'))

from message_helpers import *
from test_data_loader import load_test_data
from stats_collector import MessageStats


async def send_message_task(channel, item):
    """Send a single message asynchronously."""
    result = {'success': False, 'message_id': '', 'duration': 0, 'error': ''}
    
    try:
        message_id = extract_message_id(item)
        result['message_id'] = message_id
        target = item.get('target', 0)
        
        queue_name = f"test_queue_{target}"
        msg_start = get_current_time_ms()
        
        # Declare reply queue (exclusive)
        reply_queue = await channel.declare_queue(exclusive=True)
        
        # Create and send message
        envelope = create_data_envelope(item)
        body = serialize_envelope(envelope)
        
        future = channel.default_exchange.publish(
            aio_pika.Message(
                body=body,
                content_type='application/octet-stream',
                correlation_id=message_id,
                reply_to=reply_queue.name
            ),
            routing_key=queue_name
        )
        await future
        
        # Wait for reply
        try:
            async with reply_queue.iterator() as queue_iter:
                async for message in queue_iter:
                    async with message.process():
                        resp_envelope = parse_envelope(message.body)
                        if is_valid_ack(resp_envelope, message_id):
                            result['duration'] = get_current_time_ms() - msg_start
                            result['success'] = True
                        else:
                            result['error'] = 'Invalid ACK'
                        break # Only expect one response
        except asyncio.TimeoutError:
            result['error'] = 'Timeout'
            
    except Exception as e:
        result['error'] = str(e)
    
    return result


async def run():
    test_data = load_test_data()
    
    stats = MessageStats()
    stats.set_metadata({
        'service': 'RabbitMQ',
        'language': 'Python',
        'async': True
    })
    start_time = get_current_time_ms()
    
    print(f" [x] Starting ASYNC transfer of {len(test_data)} messages...")
    
    connection = await aio_pika.connect_robust("amqp://guest:guest@localhost/")
    
    async with connection:
        # Create channel
        channel = await connection.channel()
        
        # We need to process messages one by one or in batches as asyncio.gather 
        # with dynamic reply queues can be resource intensive.
        # For simplicity in this test pattern, we'll confirm strictly sequential async dispatch/wait
        # To make it truly parallel, we'd need shared reply queues/correlation ID map.
        # But mirroring C++ async test structure which uses futures:
        
        tasks = [send_message_task(await connection.channel(), item) for item in test_data]
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
