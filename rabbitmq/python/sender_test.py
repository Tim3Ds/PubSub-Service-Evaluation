#!/usr/bin/env python3
"""
RabbitMQ Sender with targeted routing support for multi-receiver testing.
Routes each message to test_queue_{target} based on the target field.
"""
import pika
import json
import uuid
import sys
import os
import time

# Add harness to path for stats_collector
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../harness'))
from stats_collector import MessageStats, get_current_time_ms

class RabbitMQSender:
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()
        
        # Declare queues for all 32 receivers
        for i in range(32):
            self.channel.queue_declare(queue=f'test_queue_{i}')
        
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue
        
        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)
        
        self.response = None
        self.corr_id = None

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, message_data):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        target = message_data.get('target', 0)
        queue_name = f'test_queue_{target}'
        
        self.channel.basic_publish(
            exchange='',
            routing_key=queue_name,
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=json.dumps(message_data))
        
        start_wait = time.time()
        while self.response is None and (time.time() - start_wait) < 5:
            self.connection.process_data_events()
        return self.response

def main():
    sender = RabbitMQSender()
    
    data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../test_data.json'))
    with open(data_path, 'r') as f:
        test_data = json.load(f)

    stats = MessageStats()
    stats.start_time = get_current_time_ms()

    print(f" [x] Starting transfer of {len(test_data)} messages...")

    for item in test_data:
        target = item.get('target', 0)
        msg_start = get_current_time_ms()
        print(f" [x] Sending message {item['message_id']} to target {target}...", end='', flush=True)
        
        response = sender.call(item)
        
        if response:
            try:
                resp_data = json.loads(response)
                if resp_data.get('status') == 'ACK' and resp_data.get('message_id') == item['message_id']:
                    msg_duration = get_current_time_ms() - msg_start
                    stats.record_message(True, msg_duration)
                    print(" [OK]")
                else:
                    stats.record_message(False)
                    print(f" [FAILED] Unexpected response: {response}")
            except Exception as e:
                stats.record_message(False)
                print(f" [FAILED] Parse error: {e}")
        else:
            stats.record_message(False)
            print(" [FAILED] Timeout")

    stats.end_time = get_current_time_ms()
    
    report = {
        "service": "RabbitMQ",
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

if __name__ == "__main__":
    main()
