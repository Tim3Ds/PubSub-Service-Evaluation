# Redis Messaging Service

This directory contains the Redis implementation for the messaging performance benchmark.

## Architecture

*   **Server**: Standard Redis server (downloaded and managed by `Makefile` / `test_harness.py`).
*   **Channels**: Pub/Sub mechanism using a `test` channel.
*   **Clients**: Standard Redis clients connecting to default port.

## Requirements

*   **Server**: Redis server (handled by `Makefile`).
*   **Python Client**: `redis-py`
*   **C++ Client**: `cpp_redis` (and `tacopie`).

## Build Instructions (C++)

From `redis/cpp`:
```bash
make
```

## Running Tests

Use the centralized test harness:

```bash
python3 harness/test_harness.py --service redis --sender python --py-receivers 2 --cpp-receivers 0
```

## Performance Results

(Peak throughput observed during testing)

*   **Python Sender**: ~2.25 messages/ms
