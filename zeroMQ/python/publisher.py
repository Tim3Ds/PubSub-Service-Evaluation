#!/usr/bin/env python3
"""ZeroMQ Python publisher example
Requires: pip install pyzmq
"""
import sys
import zmq

def main():
    if len(sys.argv) < 4:
        print(f"Usage: {sys.argv[0]} <bind-endpoint> <topic> <message>")
        print("Example: ./publisher.py tcp://*:5555 test hello")
        return 1
    endpoint = sys.argv[1]
    topic = sys.argv[2]
    message = sys.argv[3]

    ctx = zmq.Context()
    pub = ctx.socket(zmq.PUB)
    pub.bind(endpoint)

    # allow subscribers to connect
    import time; time.sleep(0.2)

    pub.send_string(f"{topic} {message}")
    print(f"Published on {topic}: {message}")
    return 0

if __name__ == '__main__':
    sys.exit(main())
