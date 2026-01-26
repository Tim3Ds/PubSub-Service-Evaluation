#!/usr/bin/env python3
"""
RabbitMQ Async Sender with targeted routing.
Uses aio-pika for concurrent RPC calls.
"""
import asyncio
import aio_pika
import json
import uuid
import sys
import os
import time

# Add harness to path for stats_collector
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../harness'))
from stats_collector import MessageStats, get_current_time_ms

class RabbitMQAsyncSender:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.callback_queue = None
        self.futures = {}

    async def connect(self):
        self.connection = await aio_pika.connect_robust("amqp://guest:guest@localhost/")
        self.channel = await self.connection.channel()
        
        # Declare queues for all 32 receivers
        for i in range(32):
            await self.channel.declare_queue(f'test_queue_{i}')
        
        self.callback_queue = await self.channel.declare_queue(exclusive=True)
        await self.callback_queue.consume(self.on_response)

    async def on_response(self, message: aio_pika.IncomingMessage):
        async with message.process():
            corr_id = message.correlation_id
            if corr_id in self.futures:
                self.futures[corr_id].set_result(message.body)

    async def call(self, message_data):
        corr_id = str(uuid.uuid4())
        future = asyncio.get_event_loop().create_future()
        self.futures[corr_id] = future
        
        target = message_data.get('target', 0)
        queue_name = f'test_queue_{target}'
        
        await self.channel.default_exchange.publish(
            aio_pika.Message(
                body=json.dumps(message_data).encode(),
                correlation_id=corr_id,
                reply_to=self.callback_queue.name,
            ),
            routing_key=queue_name,
        )
        
        try:
            return await asyncio.wait_for(future, timeout=0.2)
        except asyncio.TimeoutError:
            return None
        finally:
            if corr_id in self.futures:
                del self.futures[corr_id]

async def send_message(sender, item, stats):
    target = item.get('target', 0)
    msg_start = get_current_time_ms()
    print(f" [x] [ASYNC] Sending message {item['message_id']} to target {target}...")
    
    response = await sender.call(item)
    
    if response:
        try:
            resp_data = json.loads(response)
            if resp_data.get('status') == 'ACK' and resp_data.get('message_id') == item['message_id']:
                msg_duration = get_current_time_ms() - msg_start
                stats.record_message(True, msg_duration)
                print(f" [OK] Message {item['message_id']} acknowledged")
            else:
                stats.record_message(False)
                print(f" [FAILED] Unexpected response for {item['message_id']}: {response}")
        except Exception as e:
            stats.record_message(False)
            print(f" [FAILED] Parse error for {item['message_id']}: {e}")
    else:
        stats.record_message(False)
        print(f" [FAILED] Timeout for {item['message_id']}")

async def main():
    sender = RabbitMQAsyncSender()
    await sender.connect()
    
    data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../test_data.json'))
    with open(data_path, 'r') as f:
        test_data = json.load(f)

    stats = MessageStats()
    stats.start_time = get_current_time_ms()

    print(f" [x] Starting transfer of {len(test_data)} messages (ASYNC)...")

    tasks = [send_message(sender, item, stats) for item in test_data]
    await asyncio.gather(*tasks)

    stats.end_time = get_current_time_ms()
    
    report = {
        "service": "RabbitMQ",
        "language": "Python",
        "async": True,
        **stats.get_stats()
    }

    print("\nTest Results (ASYNC):")
    for k, v in report.items():
        if k != 'message_timing_stats':
            print(f"{k}: {v}")

    report_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../report.txt'))
    with open(report_path, 'a') as f:
        f.write(json.dumps(report) + "\n")

    await sender.connection.close()

if __name__ == "__main__":
    asyncio.run(main())
