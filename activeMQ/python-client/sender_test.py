#!/usr/bin/env python3
"""
ActiveMQ Sender with targeted routing support for multi-receiver testing.
Routes each message to /queue/test_request_{target} based on the target field.
"""
import stomp
import json
import sys
import os
import time
import uuid

# Add harness to path for stats_collector
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../harness'))
from stats_collector import MessageStats, get_current_time_ms

class ActiveMQSender(stomp.ConnectionListener):
    def __init__(self, host='localhost', port=61613):
        self.conn = stomp.Connection(host_and_ports=[(host, port)])
        self.conn.set_listener('sender', self)
        
        # Connection retry loop
        retries = 10
        connected = False
        while not connected and retries > 0:
            try:
                self.conn.connect(wait=True)
                connected = True
            except stomp.exception.ConnectFailedException:
                retries -= 1
                if retries > 0:
                    print(f" [!] Connection failed, retrying in 5 seconds... ({retries} retries left)")
                    time.sleep(5)
                else:
                    raise
        
        self.reply_queue = f'/queue/reply.{uuid.uuid4()}'
        self.conn.subscribe(destination=self.reply_queue, id=1, ack='auto')
        self.response = None
        self.corr_id = None

    def on_message(self, frame):
        if frame.headers.get('correlation-id') == self.corr_id:
            self.response = frame.body

    def send_and_wait(self, message_data):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        target = message_data.get('target', 0)
        destination = f'/queue/test_queue_{target}'
        
        self.conn.send(
            destination=destination,
            body=json.dumps(message_data),
            headers={
                'reply-to': self.reply_queue,
                'correlation-id': self.corr_id
            }
        )
        
        start_wait = time.time()
        while self.response is None and (time.time() - start_wait) < 0.04:
            time.sleep(0.01)
        return self.response

def main():
    sender = ActiveMQSender()
    
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
        
        response = sender.send_and_wait(item)
        
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
        "service": "ActiveMQ",
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

    sender.conn.disconnect()

if __name__ == "__main__":
    main()
