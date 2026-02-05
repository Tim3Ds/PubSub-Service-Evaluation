#!/usr/bin/env python3
"""RabbitMQ Python Sender - Sync"""
import sys
import json
import pika
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
        'service': 'RabbitMQ',
        'language': 'Python',
        'async': False
    })
    start_time = get_current_time_ms()
    
    print(f" [x] Starting transfer of {len(test_data)} messages...")
    
    # Connect to RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    
    for item in test_data:
        message_id = extract_message_id(item)
        target = item.get('target', 0)
        print(f" [x] Sending message {message_id} to target {target}...", end='', flush=True)
        
        queue_name = f"test_queue_{target}"
        msg_start = get_current_time_ms()
        
        # Declare reply queue
        result = channel.queue_declare(queue='', exclusive=True)
        callback_queue = result.method.queue
        
        # Create and send protobuf message
        envelope = create_data_envelope(item)
        body = serialize_envelope(envelope)
        
        # Send message
        channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            body=body,
            properties=pika.BasicProperties(
                reply_to=callback_queue,
                correlation_id=message_id,
                content_type='application/octet-stream'
            )
        )
        
        # Wait for reply with timeout
        response_received = False
        for method_frame, properties, reply_body in channel.consume(callback_queue, inactivity_timeout=0.04):
            if method_frame:
                # Got a response
                resp_envelope = parse_envelope(reply_body)
                if is_valid_ack(resp_envelope, message_id):
                    msg_duration = get_current_time_ms() - msg_start
                    stats.record_message(True, msg_duration)
                    print(" [OK]")
                    response_received = True
                else:
                    stats.record_message(False)
                    print(" [FAILED] Invalid ACK")
                    response_received = True
                channel.cancel()
                break
            else:
                # Timeout
                channel.cancel()
                break
        
        if not response_received:
            stats.record_message(False)
            print(" [FAILED] Timeout")
        
        # Clean up reply queue
        channel.queue_delete(queue=callback_queue)
    
    connection.close()
    
    end_time = get_current_time_ms()
    stats.set_duration(start_time, end_time)
    
    report = stats.get_stats()
    
    print("\nTest Results:")
    print(f"service: RabbitMQ")
    print(f"language: Python")
    print(f"total_sent: {stats.sent_count}")
    print(f"total_received: {stats.received_count}")
    print(f"duration_ms: {stats.get_duration_ms()}")
    
    with open('logs/report.txt', 'a') as f:
        f.write(json.dumps(report) + '\n')


if __name__ == "__main__":
    main()
