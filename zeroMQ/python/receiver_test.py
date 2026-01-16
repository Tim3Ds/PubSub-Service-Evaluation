#!/usr/bin/env python3
"""
ZeroMQ Receiver with targeted routing support for multi-receiver testing.
Uses DEALER socket with assigned identity to receive targeted messages.
Usage: python receiver_test.py --id <0-31>
"""
import zmq
import json
import argparse
import signal
import sys

class ZeroMQReceiver:
    def __init__(self, receiver_id):
        self.receiver_id = receiver_id
        self.identity = f"receiver_{receiver_id}"
        self.messages_received = 0
        self.ctx = zmq.Context()
        self.socket = self.ctx.socket(zmq.DEALER)
        self.socket.setsockopt_string(zmq.IDENTITY, self.identity)
        self.socket.connect("tcp://localhost:5555")
    
    def handle_signal(self, signum, frame):
        print(f" [x] Receiver {self.receiver_id} shutting down (received {self.messages_received} messages)")
        self.socket.close()
        self.ctx.term()
        sys.exit(0)
    
    def run(self):
        signal.signal(signal.SIGTERM, self.handle_signal)
        signal.signal(signal.SIGINT, self.handle_signal)
        
        print(f" [*] Receiver {self.receiver_id} ({self.identity}) awaiting ZeroMQ requests")

        while True:
            try:
                message = self.socket.recv_string()
                data = json.loads(message)
                message_id = data.get('message_id')
                self.messages_received += 1
                print(f" [Receiver {self.receiver_id}] Received message {message_id}")
                
                response = json.dumps({
                    "status": "ACK",
                    "message_id": message_id,
                    "receiver_id": self.receiver_id
                })
                self.socket.send_string(response)
            except Exception as e:
                print(f" [!] Error processing message: {e}")
                self.socket.send_string(json.dumps({"status": "ERROR", "message": str(e)}))

def main():
    parser = argparse.ArgumentParser(description='ZeroMQ Receiver')
    parser.add_argument('--id', type=int, default=0, help='Receiver ID (0-31)')
    args = parser.parse_args()
    
    receiver = ZeroMQReceiver(args.id)
    receiver.run()

if __name__ == "__main__":
    main()
