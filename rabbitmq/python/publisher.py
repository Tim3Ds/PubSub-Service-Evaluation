#!/usr/bin/env python3
import pika

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.exchange_declare(exchange='test_exchange', exchange_type='fanout', durable=True)

    message = "Hello from RabbitMQ Python Publisher!"
    channel.basic_publish(exchange='test_exchange', routing_key='', body=message)
    
    print(f" [x] Sent '{message}'")
    connection.close()

if __name__ == "__main__":
    main()
