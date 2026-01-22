#!/usr/bin/env python3
"""
NATS Async Receiver.
Uses nats-py to handle requests.
"""
import asyncio
from nats.aio.client import Client as NATS
import json
import argparse
import signal
import sys

class NATSAsyncReceiver:
    def __init__(self, receiver_id):
        self.receiver_id = receiver_id
        self.subject = f"test.receiver.{receiver_id}"
        self.messages_received = 0
        self.nc = NATS()
    
    async def message_handler(self, msg):
        self.messages_received += 1
        data = json.loads(msg.data.decode())
        message_id = data.get('message_id')
        print(f" [Receiver {self.receiver_id}] [ASYNC] Received message {message_id}")
        
        response = json.dumps({
            "status": "ACK",
            "message_id": message_id,
            "receiver_id": self.receiver_id,
            "async": True
        })
        await self.nc.publish(msg.reply, response.encode())

    async def run(self):
        await self.nc.connect(servers=["nats://localhost:4222"])
        print(f" [*] [ASYNC] Receiver {self.receiver_id} subscribed to {self.subject}")
        await self.nc.subscribe(self.subject, cb=self.message_handler)
        
        # Keep running until cancelled
        while True:
            await asyncio.sleep(1)

async def main():
    parser = argparse.ArgumentParser(description='NATS Async Receiver')
    parser.add_argument('--id', type=int, default=0, help='Receiver ID (0-31)')
    args = parser.parse_args()
    
    receiver = NATSAsyncReceiver(args.id)
    
    # Setup signal handlers
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown(receiver)))

    try:
        await receiver.run()
    except Exception as e:
        print(f" [!] [ASYNC] Receiver error: {e}")

async def shutdown(receiver):
    print(f" [x] [ASYNC] Shutdown requested for NATS receiver {receiver.receiver_id} (received {receiver.messages_received} messages)")
    await receiver.nc.close()
    sys.exit(0)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
