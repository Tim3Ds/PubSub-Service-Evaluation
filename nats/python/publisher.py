#!/usr/bin/env python3.12

# Simple NATS Python Publisher
# Requires: pip install nats-py
import asyncio
from nats.aio.client import Client as NATS
import sys

async def main():
    if len(sys.argv) < 4:
        print(f"Usage: {sys.argv[0]} <server> <subject> <message>")
        return
    server = sys.argv[1]
    subject = sys.argv[2]
    message = sys.argv[3]

    nc = NATS()
    await nc.connect(servers=[server])
    await nc.publish(subject, message.encode())
    print(f"Published to {subject}: {message}")
    await nc.drain()

if __name__ == "__main__":
    asyncio.run(main())
