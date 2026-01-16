#!/usr/bin/env python3
"""
RabbitMQ Receiver with targeted routing support for multi-receiver testing.
Each receiver listens on test_queue_{id} based on its assigned ID.
Usage: python receiver_test.py --id <0-31>
"""
import pika
import json
import argparse

def main():
    parser = argparse.ArgumentParser(description='RabbitMQ Receiver')
    parser.add_argument('--id', type=int, default=0, help='Receiver ID (0-31)')
    args = parser.parse_args()
    
    receiver_id = args.id
    queue_name = f'test_queue_{receiver_id}'
    
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.queue_declare(queue=queue_name)

    def on_request(ch, method, props, body):
        try:
            data = json.loads(body)
            message_id = data.get('message_id')
            
            response = json.dumps({
                "status": "ACK",
                "message_id": message_id,
                "receiver_id": receiver_id
            })
            
            ch.basic_publish(exchange='',
                             routing_key=props.reply_to,
                             properties=pika.BasicProperties(correlation_id=props.correlation_id),
                             body=response)
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            print(f" [!] Error processing message: {e}")

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=queue_name, on_message_callback=on_request)

    print(f" [*] Receiver {receiver_id} awaiting messages on {queue_name}")
    channel.start_consuming()

if __name__ == "__main__":
    main()
