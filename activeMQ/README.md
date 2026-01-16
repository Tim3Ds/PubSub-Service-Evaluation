# ActiveMQ Messaging Service

This directory contains the ActiveMQ implementation for the messaging performance benchmark.

## Architecture

*   **Broker**: ActiveMQ 6.2.0 (Classic) running on Java 17.
*   **Protocol**: STOMP over TCP (port 61613).
*   **Topic**: `test` topic.

## Requirements

*   **Broker**: Apache ActiveMQ 6.2.0 (binaries included/downloaded).
*   **Java**: Requires **Java 17+**. The harness defaults to `/usr/lib/jvm/java-17-openjdk...` if available.
*   **Python Client**: `stomp.py`
*   **C++ Client**: `activemq-cpp`.

## Build Instructions (C++)

From `activeMQ/cpp-client`:
(See specific build instructions within the subdirectory if utilizing the custom build script)

## Running Tests

Use the centralized test harness. Note that ActiveMQ takes ~15s to start.

```bash
python3 harness/test_harness.py --service activemq --sender python --py-receivers 2 --cpp-receivers 0
```

## Performance Results

(Peak throughput observed during testing)

*   **Python Sender**: ~0.09 messages/ms
