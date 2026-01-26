#!/usr/bin/env python3
"""
ActiveMQ Async Sender with targeted routing.
Uses threads to send messages concurrently.
"""
import stomp
import json
import sys
import os
import time
import uuid
import threading

# Add harness to path for stats_collector
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../harness'))
from stats_collector import MessageStats, get_current_time_ms

class ActiveMQAsyncSender(stomp.ConnectionListener):
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
                    time.sleep(5)
                else:
                    raise
        
        self.reply_queue = f'/queue/reply.{uuid.uuid4()}'
        self.conn.subscribe(destination=self.reply_queue, id=1, ack='auto')
        self.futures = {} # corr_id -> {event: threading.Event, response: data}

    def on_message(self, frame):
        corr_id = frame.headers.get('correlation-id')
        if corr_id in self.futures:
            self.futures[corr_id]['response'] = frame.body
            self.futures[corr_id]['event'].set()

    def send_and_wait(self, message_data):
        corr_id = str(uuid.uuid4())
        event = threading.Event()
        self.futures[corr_id] = {'event': event, 'response': None}
        
        target = message_data.get('target', 0)
        destination = f'/queue/test_queue_{target}'
        
        self.conn.send(
            destination=destination,
            body=json.dumps(message_data),
            headers={
                'reply-to': self.reply_queue,
                'correlation-id': corr_id
            }
        )
        
        if event.wait(timeout=0.04):
            res = self.futures[corr_id]['response']
            del self.futures[corr_id]
            return res
        else:
            del self.futures[corr_id]
            return None

def send_message_thread(sender, item, stats):
    target = item.get('target', 0)
    msg_start = get_current_time_ms()
    print(f" [x] [ASYNC] Sending message {item['message_id']} to target {target}...")
    
    response = sender.send_and_wait(item)
    
    if response:
        try:
            resp_data = json.loads(response)
            if resp_data.get('status') == 'ACK' and resp_data.get('message_id') == item['message_id']:
                msg_duration = get_current_time_ms() - msg_start
                stats.record_message(True, msg_duration)
                print(f" [OK] Message {item['message_id']} acknowledged")
            else:
                stats.record_message(False)
                print(f" [FAILED] Unexpected response for {item['message_id']}: {response}")
        except Exception as e:
            stats.record_message(False)
            print(f" [FAILED] Parse error for {item['message_id']}: {e}")
    else:
        stats.record_message(False)
        print(f" [FAILED] Timeout for {item['message_id']}")

def main():
    sender = ActiveMQAsyncSender()
    
    data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../test_data.json'))
    with open(data_path, 'r') as f:
        test_data = json.load(f)

    stats = MessageStats()
    stats.start_time = get_current_time_ms()

    print(f" [x] Starting transfer of {len(test_data)} messages (ASYNC)...")

    threads = []
    for item in test_data:
        t = threading.Thread(target=send_message_thread, args=(sender, item, stats))
        t.start()
        threads.append(t)
        # Limit concurrency slightly to avoid overwhelming ActiveMQ if needed
        # time.sleep(0.01) 

    for t in threads:
        t.join()

    stats.end_time = get_current_time_ms()
    
    report = {
        "service": "ActiveMQ",
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

    sender.conn.disconnect()

if __name__ == "__main__":
    main()
