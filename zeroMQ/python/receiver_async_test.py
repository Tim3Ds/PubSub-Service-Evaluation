#!/usr/bin/env python3
"""
ZeroMQ Async Receiver with P2P support.
Uses zmq.asyncio for non-blocking message handling.
"""
import zmq
import zmq.asyncio
import json
import argparse
import signal
import sys
import asyncio

class ZeroMQAsyncReceiver:
    def __init__(self, receiver_id):
        self.receiver_id = receiver_id
        self.port = 5556 + receiver_id
        self.messages_received = 0
        self.ctx = zmq.asyncio.Context()
        self.socket = self.ctx.socket(zmq.REP)
        self.socket.bind(f"tcp://*:{self.port}")
        self.running = True
    
    def handle_signal(self, signum, frame):
        print(f" [x] [ASYNC] Receiver {self.receiver_id} shutting down (received {self.messages_received} messages)")
        self.running = False
        # In a real async app we might need to break the loop more gracefully
    
    async def run(self):
        print(f" [*] [ASYNC] Receiver {self.receiver_id} bound to port {self.port}, awaiting requests")

        while self.running:
            try:
                # Use wait_for to allow checking self.running or just use recv_string
                message = await self.socket.recv_string()
                data = json.loads(message)
                message_id = data.get('message_id')
                self.messages_received += 1
                print(f" [Receiver {self.receiver_id}] [ASYNC] Received message {message_id}")
                
                # Simulate some async work if needed
                # await asyncio.sleep(0.01) 
                
                response = json.dumps({
                    "status": "ACK",
                    "message_id": message_id,
                    "receiver_id": self.receiver_id,
                    "async": True
                })
                await self.socket.send_string(response)
            except zmq.error.ContextTerminated:
                break
            except Exception as e:
                print(f" [!] [ASYNC] Error processing message: {e}")
                try:
                    await self.socket.send_string(json.dumps({"status": "ERROR", "message": str(e)}))
                except:
                    pass

async def main():
    parser = argparse.ArgumentParser(description='ZeroMQ Async Receiver')
    parser.add_argument('--id', type=int, default=0, help='Receiver ID (0-31)')
    args = parser.parse_args()
    
    receiver = ZeroMQAsyncReceiver(args.id)
    
    # Setup signal handlers
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown(receiver)))

    await receiver.run()

async def shutdown(receiver):
    print(f" [x] [ASYNC] Shutdown requested for receiver {receiver.receiver_id}")
    receiver.running = False
    receiver.socket.close()
    receiver.ctx.term()
    sys.exit(0)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
