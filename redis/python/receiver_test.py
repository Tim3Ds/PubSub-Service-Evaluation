#!/usr/bin/env python3
"""
Redis Receiver with targeted routing support for multi-receiver testing.
Each receiver listens on test_queue_{id} based on its assigned ID.
"""
import redis
import json
import sys
import os
import argparse

def main():
    parser = argparse.ArgumentParser(description='Redis Receiver')
    parser.add_argument('--id', type=int, default=0, help='Receiver ID (0-31)')
    args = parser.parse_args()
    
    receiver_id = args.id
    r = redis.Redis(host='localhost', port=6379, db=0)
    queue_name = f'test_queue_{receiver_id}'
    
    print(f" [*] Receiver {receiver_id} waiting for messages on {queue_name}. To exit press CTRL+C")
    
    try:
        while True:
            # BLPOP waits for an item from the list
            result = r.blpop(queue_name, timeout=1)
            if result is None:
                continue
                
            _, message = result
            data = json.loads(message)
            
            message_id = data.get('message_id')
            reply_to = data.get('reply_to')
            
            if message_id and reply_to:
                # print(f" [x] Receiver {receiver_id}: Received {message_id}, replying to {reply_to}")
                response = {
                    "status": "ACK",
                    "message_id": message_id,
                    "receiver_id": receiver_id
                }
                r.rpush(reply_to, json.dumps(response))
                r.expire(reply_to, 60)
    except KeyboardInterrupt:
        print(f"Stopping receiver {receiver_id}...")

if __name__ == "__main__":
    main()
