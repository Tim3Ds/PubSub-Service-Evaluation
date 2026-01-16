# RabbitMQ Messaging Service

This directory contains the RabbitMQ implementation for the messaging performance benchmark.

## Architecture

*   **Broker**: Standard RabbitMQ server (downloaded and managed by `Makefile` / `test_harness.py`).
*   **Exchange**: Topic exchange named `test_exchange`.
*   **Queues**: One queue per receiver, bound to `test_exchange` with routing key `test`.

## Requirements

*   **Server**: RabbitMQ server (handled by `Makefile`).
*   **Python Client**: `pika`
*   **C++ Client**: `SimpleAmqpClient` (builds `rabbitmq-c` dependency).

## Build Instructions (C++)

From `rabbitmq/cpp`:
```bash
make
```

## Running Tests

Use the test harness to automatically start the broker and run clients:

```bash
python3 harness/test_harness.py --service rabbitmq --sender python --py-receivers 2 --cpp-receivers 0
```

## Performance Results

(Peak throughput observed during testing)

*   **Python Sender**: ~1.58 messages/ms
