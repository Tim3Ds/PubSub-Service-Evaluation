#!/usr/bin/env python3
"""ActiveMQ Python Receiver - Async"""
import sys
import signal
import asyncio
import stomp
import threading
from pathlib import Path

# Add utils to path
repo_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(repo_root / 'utils' / 'python'))

from message_helpers import *

running = True

def signal_handler(sig, frame):
    global running
    running = False

class AsyncRequestListener(stomp.ConnectionListener):
    def __init__(self, conn, receiver_id):
        self.conn = conn
        self.receiver_id = receiver_id
        
    def on_message(self, frame):
        # We need to process this off the stomp thread to be truly "async" 
        # but for this test simple response on same thread is OK as STOMP lib handles threading
        try:
            # Body should now be bytes since auto_decode=False
            body = frame.body
            if isinstance(body, str):
                body = body.encode('latin-1')  # latin-1 preserves bytes 0-255
                
            request_envelope = parse_envelope(body)
            message_id = request_envelope.message_id
            print(f" [x] [ASYNC] Received message {message_id}")
            
            # Create ACK
            response = create_ack_from_envelope(request_envelope, str(self.receiver_id))
            setattr(response, 'async', True)
            resp_str = serialize_envelope(response)
            
            # Send reply
            if 'reply-to' in frame.headers:
                self.conn.send(
                    destination=frame.headers['reply-to'],
                    body=resp_str,
                    headers={
                        'correlation-id': frame.headers.get('correlation-id'),
                        'content-type': 'application/octet-stream'
                    }
                )
        except Exception as e:
            print(f"Error: {e}")

async def run(receiver_id):
    conn = stomp.Connection([('localhost', 61613)], auto_decode=False)
    listener = AsyncRequestListener(conn, receiver_id)
    conn.set_listener('', listener)
    
    # Run connect in executor to avoid blocking loop
    await asyncio.get_event_loop().run_in_executor(None, lambda: conn.connect('admin', 'admin', wait=True))
    
    dest = f"/queue/test_queue_{receiver_id}"
    conn.subscribe(destination=dest, id=1, ack='auto')
    
    print(f" [*] [ASYNC] Receiver {receiver_id} waiting for messages on {dest}")
    
    while running:
        await asyncio.sleep(0.1)
        
    print(f" [x] [ASYNC] Receiver {receiver_id} shutting down")
    conn.disconnect()


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--id', type=int, default=0)
    args = parser.parse_args()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(run(args.id))
    except (KeyboardInterrupt, SystemExit):
        pass
    finally:
        loop.close()


if __name__ == "__main__":
    main()
