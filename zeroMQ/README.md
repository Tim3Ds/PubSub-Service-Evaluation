# ZeroMQ Implementation

This folder contains the ZeroMQ messaging implementation using the **ROUTER-DEALER** pattern.

## Architecture

*   **Router (`python/router.py`)**: A central broker that manages routing between senders and receivers. It binds to `tcp://*:5555`.
*   **Sender**: connects to the Router using a `DEALER` socket with identity "sender".
*   **Receiver**: connects to the Router using a `DEALER` socket with identity "receiver_<ID>".

**Note**: The lifecycle of the Router is now managed automatically by the `test_harness.py`.

## Requirements

*   **C++ API**: `libzmq` and `cppzmq` headers (installed via `make` in `zeroMQ/cpp`).
*   **Python API**: `pyzmq` (`pip install pyzmq`).

## Build Instructions (C++)

From the `zeroMQ/cpp` directory:

```bash
make build
# Binary created at zeroMQ/cpp/build/bin/sender_test and receiver_test
```

## Running Tests

Use the centralized test harness to run tests, which handles starting the intermediate Router automatically:

```bash
# Example: Python sender -> 2 Python receivers
python3 harness/test_harness.py --service zeromq --sender python --py-receivers 2 --cpp-receivers 0

# Example: C++ sender -> 2 C++ receivers
python3 harness/test_harness.py --service zeromq --sender cpp --py-receivers 0 --cpp-receivers 2
```

## Performance Results

(Peak throughput observed during testing)

*   **Python Sender**: ~1.67 messages/ms
*   **C++ Sender**: ~1.68 messages/ms
