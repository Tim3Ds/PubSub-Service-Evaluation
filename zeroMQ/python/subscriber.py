#!/usr/bin/env python3
"""ZeroMQ Python subscriber example
Requires: pip install pyzmq
"""
import sys
import zmq

def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <connect-endpoint> <topic>")
        print("Example: ./subscriber.py tcp://localhost:5555 test")
        return 1
    endpoint = sys.argv[1]
    topic = sys.argv[2]

    ctx = zmq.Context()
    sub = ctx.socket(zmq.SUB)
    sub.connect(endpoint)
    sub.setsockopt_string(zmq.SUBSCRIBE, topic)

    try:
        while True:
            msg = sub.recv_string()
            print(f"Received: {msg}")
    except KeyboardInterrupt:
        pass
    return 0

if __name__ == '__main__':
    sys.exit(main())
