#!/usr/bin/env python3
"""
ActiveMQ Receiver with targeted routing support for multi-receiver testing.
Each receiver subscribes to /queue/test_request_{id} based on its assigned ID.
Usage: python receiver_test.py --id <0-31>
"""
import stomp
import json
import time
import argparse

class ActiveMQReceiver(stomp.ConnectionListener):
    def __init__(self, conn, receiver_id):
        self.conn = conn
        self.receiver_id = receiver_id

    def on_message(self, frame):
        try:
            data = json.loads(frame.body)
            message_id = data.get('message_id')
            
            response = json.dumps({
                "status": "ACK",
                "message_id": message_id,
                "receiver_id": self.receiver_id
            })
            reply_to = frame.headers.get('reply-to')
            corr_id = frame.headers.get('correlation-id')
            
            if reply_to:
                self.conn.send(
                    destination=reply_to,
                    body=response,
                    headers={'correlation-id': corr_id}
                )
        except Exception as e:
            print(f" [!] Error: {e}")

def main():
    parser = argparse.ArgumentParser(description='ActiveMQ Receiver')
    parser.add_argument('--id', type=int, default=0, help='Receiver ID (0-31)')
    args = parser.parse_args()
    
    receiver_id = args.id
    destination = f'/queue/test_queue_{receiver_id}'
    
    conn = stomp.Connection(host_and_ports=[('localhost', 61613)])
    receiver = ActiveMQReceiver(conn, receiver_id)
    conn.set_listener('receiver', receiver)
    conn.connect(wait=True)
    conn.subscribe(destination=destination, id=1, ack='auto')

    print(f" [*] Receiver {receiver_id} awaiting ActiveMQ requests on {destination}")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    conn.disconnect()

if __name__ == "__main__":
    main()
