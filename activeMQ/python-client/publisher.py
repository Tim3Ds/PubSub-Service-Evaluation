#!/usr/bin/env python3
"""Simple STOMP publisher for ActiveMQ.

Usage:
  pip install -r requirements.txt
  python publisher.py [host] [port] [destination]

Defaults: host=localhost port=61613 destination=/queue/test
"""
import sys
import stomp

host = sys.argv[1] if len(sys.argv) > 1 else 'localhost'
port = int(sys.argv[2]) if len(sys.argv) > 2 else 61613
# Use a topic so all subscribers get the same message
destination = sys.argv[3] if len(sys.argv) > 3 else '/topic/test'

conn = stomp.Connection(host_and_ports=[(host, port)])
try:
    conn.connect(wait=True)
except Exception as e:
    print('Connection failed:', e)
    raise

# for i in range(0, 10):
message = f"Hello from Python (stomp.py)"
conn.send(destination=destination, body=message)
print(f'Sent to {destination} on {host}:{port}: {message}')
conn.disconnect()
