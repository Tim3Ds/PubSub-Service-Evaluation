#!/usr/bin/env python3
"""
ActiveMQ Async Receiver.
Uses stomp.py ConnectionListener.
"""
import stomp
import json
import argparse
import signal
import sys
import time

class ActiveMQAsyncReceiver(stomp.ConnectionListener):
    def __init__(self, receiver_id):
        self.receiver_id = receiver_id
        self.queue_name = f'/queue/test_queue_{receiver_id}'
        self.messages_received = 0
        self.conn = stomp.Connection(host_and_ports=[('localhost', 61613)])
        self.conn.set_listener('receiver', self)
        self.running = True

    def on_message(self, frame):
        self.messages_received += 1
        data = json.loads(frame.body)
        message_id = data.get('message_id')
        reply_to = frame.headers.get('reply-to')
        corr_id = frame.headers.get('correlation-id')
        
        print(f" [Receiver {self.receiver_id}] [ASYNC] Received message {message_id}")
        
        response = json.dumps({
            "status": "ACK",
            "message_id": message_id,
            "receiver_id": self.receiver_id,
            "async": True
        })
        
        if reply_to:
            self.conn.send(
                destination=reply_to,
                body=response,
                headers={'correlation-id': corr_id}
            )

    def run(self):
        self.conn.connect(wait=True)
        self.conn.subscribe(destination=self.queue_name, id=1, ack='auto')
        print(f" [*] [ASYNC] Receiver {self.receiver_id} awaiting messages on {self.queue_name}")
        
        while self.running:
            time.sleep(1)

def shutdown(receiver):
    print(f" [x] [ASYNC] Shutdown requested for ActiveMQ receiver {receiver.receiver_id} (received {receiver.messages_received} messages)")
    receiver.running = False
    receiver.conn.disconnect()
    sys.exit(0)

def main():
    parser = argparse.ArgumentParser(description='ActiveMQ Async Receiver')
    parser.add_argument('--id', type=int, default=0, help='Receiver ID (0-31)')
    args = parser.parse_args()
    
    receiver = ActiveMQAsyncReceiver(args.id)
    
    signal.signal(signal.SIGINT, lambda s, f: shutdown(receiver))
    signal.signal(signal.SIGTERM, lambda s, f: shutdown(receiver))

    try:
        receiver.run()
    except Exception as e:
        print(f" [!] [ASYNC] Receiver error: {e}")

if __name__ == "__main__":
    main()
