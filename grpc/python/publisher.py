#!/usr/bin/env python3

import asyncio
import grpc
import pubsub_pb2
import pubsub_pb2_grpc
import sys

async def publish():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <topic> <values>")
        return
    topic = sys.argv[1]
    values_str = sys.argv[2]  # e.g., "1238,1235,5285,5456,5000,0,0"

    async with grpc.aio.insecure_channel('localhost:50051') as channel:
        stub = pubsub_pb2_grpc.PubSubServiceStub(channel)
        async def request_generator():
            # Send message
            msg = pubsub_pb2.Message(topic=topic)
            for val_str in values_str.split(','):
                val = pubsub_pb2.Value(int_value=int(val_str))
                msg.values.append(val)
            yield msg
            # Keep stream open
            while True:
                await asyncio.sleep(1)

        responses = stub.SubscribeAndPublish(request_generator())
        async for response in responses:
            print(f"Received on {response.topic}: {response.values}")

if __name__ == '__main__':
    asyncio.run(publish())
