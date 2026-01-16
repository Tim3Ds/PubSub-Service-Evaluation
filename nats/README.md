# NATS Messaging Service

This directory contains the NATS implementation for the messaging performance benchmark.

## Architecture

*   **Server**: NATS server (downloaded and managed by `Makefile` / `test_harness.py`).
*   **Subject**: Messages published to `updates` subject.
*   **Clients**: NATS clients connecting to default port `4222`.

## Requirements

*   **Server**: NATS server (handled by `Makefile`).
*   **Python Client**: `nats-py`
*   **C++ Client**: `nats.c` / `nats-c`.

## Build Instructions (C++)

From `nats/cpp`:
```bash
make
```

## Running Tests

Use the centralized test harness:

```bash
python3 harness/test_harness.py --service nats --sender python --py-receivers 2 --cpp-receivers 0
```

## Performance Results

(Peak throughput observed during testing)

*   **Python Sender**: ~1.95 messages/ms
