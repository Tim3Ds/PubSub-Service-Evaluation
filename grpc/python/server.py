#!/usr/bin/env python3

import asyncio
import grpc
from concurrent import futures
import pubsub_pb2
import pubsub_pb2_grpc

class PubSubService(pubsub_pb2_grpc.PubSubServiceServicer):
    def __init__(self):
        self.subscribers = {}  # topic -> list of queues

    async def SubscribeAndPublish(self, request_iterator, context):
        queue = asyncio.Queue()
        subscription_topics = set()

        async def read_messages():
            try:
                async for message in request_iterator:
                    topic = message.topic
                    if topic not in self.subscribers:
                        self.subscribers[topic] = []
                    
                    if queue not in self.subscribers[topic]:
                        self.subscribers[topic].append(queue)
                        subscription_topics.add(topic)
                        print(f"New subscription to {topic}")
                    
                    if message.values: 
                         sub_queues = self.subscribers.get(topic, [])
                         for q in sub_queues:
                             q.put_nowait(message)
            except Exception as e:
                print(f"Read error: {e}")
            finally:
                for t in subscription_topics:
                    if t in self.subscribers and queue in self.subscribers[t]:
                        self.subscribers[t].remove(queue)
        
        read_task = asyncio.create_task(read_messages())
        
        try:
            while True:
                # Wait for queue item or read_task completion
                get_task = asyncio.ensure_future(queue.get())
                done, pending = await asyncio.wait(
                    [get_task, read_task], 
                    return_when=asyncio.FIRST_COMPLETED
                )
                
                if get_task in done:
                    yield get_task.result()
                else:
                    get_task.cancel()
                
                if read_task in done:
                    # Connection closed by client
                    break
                    
        except Exception as e:
            print(f"Write error: {e}")
        finally:
            read_task.cancel()
            for t in subscription_topics:
                if t in self.subscribers and queue in self.subscribers[t]:
                    self.subscribers[t].remove(queue)

async def serve():
    server = grpc.aio.server()
    pubsub_pb2_grpc.add_PubSubServiceServicer_to_server(PubSubService(), server)
    server.add_insecure_port('[::]:50051')
    print("Starting server on port 50051")
    await server.start()
    await server.wait_for_termination()

if __name__ == '__main__':
    asyncio.run(serve())
