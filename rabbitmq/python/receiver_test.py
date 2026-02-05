#!/usr/bin/env python3
"""RabbitMQ Python Receiver - Sync"""
import sys
import signal
import pika
from pathlib import Path

# Add utils to path
repo_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(repo_root / 'utils' / 'python'))

from message_helpers import *

running = True

def signal_handler(sig, frame):
    global running
    running = False

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--id', type=int, default=0)
    args = parser.parse_args()
    
    receiver_id = args.id
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    
    queue_name = f"test_queue_{receiver_id}"
    channel.queue_declare(queue=queue_name)
    
    print(f" [*] Receiver {receiver_id} waiting for messages on {queue_name}")
    
    # Process messages with basic_get (non-blocking) to yield to signal handler
    # or ensure we check running flag
    
    def on_message(ch, method, properties, body):
        request_envelope = parse_envelope(body)
        message_id = request_envelope.message_id
        print(f" [x] Received message {message_id}")
        
        # Create ACK
        response = create_ack_from_envelope(request_envelope, str(receiver_id))
        resp_str = serialize_envelope(response)
        
        # Send reply
        ch.basic_publish(
            exchange='',
            routing_key=properties.reply_to,
            body=resp_str,
            properties=pika.BasicProperties(
                correlation_id=properties.correlation_id,
                content_type='application/octet-stream'
            )
        )
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=queue_name, on_message_callback=on_message)
    
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        pass
    
    print(f" [x] Receiver {receiver_id} shutting down")
    connection.close()


if __name__ == "__main__":
    main()
