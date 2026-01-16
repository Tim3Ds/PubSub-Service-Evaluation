#!/usr/bin/env python3
"""
ZeroMQ Router - Central message router for multi-receiver testing.
Routes messages from sender to specific receivers based on target ID.
"""
import zmq

def main():
    ctx = zmq.Context()
    router = ctx.socket(zmq.ROUTER)
    router.bind("tcp://*:5555")

    print(" [*] ZeroMQ Router started on port 5555")

    while True:
        try:
            # Receive message from any client
            frames = router.recv_multipart()
            
            if len(frames) >= 3:
                # From sender: [sender_id, target_id, message]
                sender_id = frames[0]
                target_id = frames[1]
                message = frames[2]
                print(f" [Router] Forwarding from {sender_id} to {target_id}")
                
                # Forward to target receiver
                router.send_multipart([target_id, message])
                
            elif len(frames) == 2:
                # Response from receiver: [receiver_id, response]
                receiver_id = frames[0]
                response = frames[1]
                print(f" [Router] Response from {receiver_id} to sender")
                
                # Forward back to sender
                router.send_multipart([b"sender", response])
            else:
                print(f" [Router] Received unknown multipart: {len(frames)} frames")
                
        except Exception as e:
            print(f" [!] Router error: {e}")

if __name__ == "__main__":
    main()
