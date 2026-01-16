#!/usr/bin/env python3

import asyncio
import grpc
import pubsub_pb2
import pubsub_pb2_grpc
import sys

async def subscribe():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <topic>")
        return
    topic = sys.argv[1]

    async with grpc.aio.insecure_channel('localhost:50051') as channel:
        stub = pubsub_pb2_grpc.PubSubServiceStub(channel)
        async def request_generator():
            # Send subscription message
            message = pubsub_pb2.Message(topic=topic, values=[])
            yield message
            # Keep stream open
            while True:
                await asyncio.sleep(1)

        responses = stub.SubscribeAndPublish(request_generator())
        async for response in responses:
            print(f"Received on {response.topic}: {response.values}")

if __name__ == '__main__':
    asyncio.run(subscribe())
