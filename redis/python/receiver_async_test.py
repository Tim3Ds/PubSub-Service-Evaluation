#!/usr/bin/env python3
"""
Redis Async Receiver.
Uses redis-py async support to handle requests.
"""
import asyncio
import redis.asyncio as redis
import json
import argparse
import signal
import sys

class RedisAsyncReceiver:
    def __init__(self, receiver_id):
        self.receiver_id = receiver_id
        self.queue_name = f"test_queue_{receiver_id}"
        self.messages_received = 0
        self.r = redis.Redis(host='localhost', port=6379, db=0)
        self.running = True

    async def run(self):
        print(f" [*] [ASYNC] Receiver {self.receiver_id} awaiting messages on {self.queue_name}")

        while self.running:
            try:
                # BLPOP returns (queue_name, data)
                response = await self.r.blpop(self.queue_name, timeout=1)
                if response:
                    self.messages_received += 1
                    data = json.loads(response[1])
                    message_id = data.get('message_id')
                    reply_to = data.get('reply_to')
                    
                    print(f" [Receiver {self.receiver_id}] [ASYNC] Received message {message_id}")
                    
                    ack_data = json.dumps({
                        "status": "ACK",
                        "message_id": message_id,
                        "receiver_id": self.receiver_id,
                        "async": True
                    })
                    
                    if reply_to:
                        await self.r.rpush(reply_to, ack_data)
            except Exception as e:
                if self.running:
                    print(f" [!] [ASYNC] Error processing message: {e}")

async def shutdown(receiver):
    print(f" [x] [ASYNC] Shutdown requested for Redis receiver {receiver.receiver_id} (received {receiver.messages_received} messages)")
    receiver.running = False
    await receiver.r.aclose()
    sys.exit(0)

async def main():
    parser = argparse.ArgumentParser(description='Redis Async Receiver')
    parser.add_argument('--id', type=int, default=0, help='Receiver ID (0-31)')
    args = parser.parse_args()
    
    receiver = RedisAsyncReceiver(args.id)
    
    # Setup signal handlers
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown(receiver)))

    try:
        await receiver.run()
    except Exception as e:
        print(f" [!] [ASYNC] Receiver error: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
