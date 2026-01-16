#!/usr/bin/env python3.12

# Simple NATS Python Subscriber
# Requires: pip install nats-py
import asyncio
from nats.aio.client import Client as NATS
import sys

async def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <server> <subject>")
        return
    server = sys.argv[1]
    subject = sys.argv[2]

    async def message_handler(msg):
        print(f"Received on [{msg.subject}]: {msg.data.decode()}")

    nc = NATS()
    await nc.connect(servers=[server])
    await nc.subscribe(subject, cb=message_handler)
    print(f"Subscribed to {subject}. Press Ctrl+C to exit.")
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        pass
    await nc.drain()

if __name__ == "__main__":
    asyncio.run(main())
