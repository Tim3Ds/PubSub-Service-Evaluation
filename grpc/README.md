# gRPC Messaging Service

This directory contains the gRPC implementation for the messaging performance benchmark.

## Architecture

*   **Central Server (`python/server.py`)**: A centralized Pub/Sub server managed by `test_harness.py`. Listens on port **50051**.
*   **Sender**: Connects to the central server to publish messages.
*   **Receiver**: Subscribes to topics on the central server.
*   **Proto Definition**: `proto/pubsub.proto`.

## Structure

*   `cpp/`: C++ implementation (Sender/Receiver)
    *   `build/`: Build directory (use `make` to build)
*   `python/`: Python implementation
*   `proto/`: Protocol buffer definitions

## Build Instructions (C++)

Run `make` in `cpp/` directory. It will automatically download and build `protobuf` and `grpc` dependencies from source.

## Running Tests

Use the centralized test harness:

```bash
# Example: Python sender -> 2 Python receivers
python3 harness/test_harness.py --service grpc --sender python --py-receivers 2 --cpp-receivers 0
```

## Performance Results

(Peak throughput observed during testing)

*   **Python Sender**: ~2.27 messages/ms
