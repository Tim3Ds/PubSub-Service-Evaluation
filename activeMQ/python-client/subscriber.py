#!/usr/bin/env python3
"""Simple STOMP subscriber for ActiveMQ.

Usage:
  pip install -r requirements.txt
  python subscriber.py [host] [port] [destination]

Defaults: host=localhost port=61613 destination=/queue/test
"""
import sys
import time
import stomp

class PrintListener(stomp.ConnectionListener):
    def on_error(self, frame):
        print('Error:', frame.body)
    def on_message(self, frame):
        print('Received:', frame.body)

host = sys.argv[1] if len(sys.argv) > 1 else 'localhost'
port = int(sys.argv[2]) if len(sys.argv) > 2 else 61613
# Use a topic so all subscribers get the same message
destination = sys.argv[3] if len(sys.argv) > 3 else '/topic/test'

conn = stomp.Connection(host_and_ports=[(host, port)])
conn.set_listener('', PrintListener())
try:
    conn.connect(wait=True)
except Exception as e:
    print('Connection failed:', e)
    raise

conn.subscribe(destination=destination, id=1, ack='auto')
print(f'Subscribed to {destination} on {host}:{port}. Waiting for messages...')
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print('\nInterrupted, disconnecting')
finally:
    conn.disconnect()
