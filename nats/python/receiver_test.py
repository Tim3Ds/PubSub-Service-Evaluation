#!/usr/bin/env python3.12
"""
NATS Receiver with targeted routing support for multi-receiver testing.
Each receiver subscribes to test.receiver.{id} based on its assigned ID.
Usage: python receiver_test.py --id <0-31>
"""
import asyncio
from nats.aio.client import Client as NATS
import json
import argparse

async def main():
    parser = argparse.ArgumentParser(description='NATS Receiver')
    parser.add_argument('--id', type=int, default=0, help='Receiver ID (0-31)')
    args = parser.parse_args()
    
    receiver_id = args.id
    subject = f"test.receiver.{receiver_id}"
    
    nc = NATS()
    await nc.connect(servers=["nats://localhost:4222"])

    async def message_handler(msg):
        try:
            data = json.loads(msg.data.decode())
            message_id = data.get('message_id')
            
            response = json.dumps({
                "status": "ACK",
                "message_id": message_id,
                "receiver_id": receiver_id
            })
            await nc.publish(msg.reply, response.encode())
        except Exception as e:
            print(f" [!] Error processing message: {e}")

    await nc.subscribe(subject, cb=message_handler)
    print(f" [*] Receiver {receiver_id} awaiting NATS requests on {subject}")

    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        pass
    await nc.close()

if __name__ == "__main__":
    asyncio.run(main())
