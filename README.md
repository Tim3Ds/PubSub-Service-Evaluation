# Messaging Performance Benchmark Suite

A comprehensive PubSub performance analysis and benchmarking tool for major messaging systems, supporting both **Python** and **C++** implementations. This project evaluates systems like Redis, RabbitMQ, ZeroMQ, NATS, gRPC, and ActiveMQ under various workloads, including multi-receiver stress tests.

## 🚀 Features

- **Multi-System Support**: Benchmarks for Redis, RabbitMQ, ZeroMQ, NATS, gRPC, and ActiveMQ.
- **Cross-Language Interoperability**: Senders and receivers implemented in both Python and C++.
- **Multi-Receiver Stress Testing**: Simulate real-world distributed workloads with up to 32 concurrent receivers.
- **Reliable Message Transfer**: Implements request-response patterns or explicit acknowledgments to ensure data integrity.
- **Automated Reporting**: Generates JSON reports and consolidated logs for performance analysis (throughput, latency, success rates).
- **Targeted Routing**: Messages can be routed to specific receivers using a `target` ID field.

## 🏗 Project Structure

```text
.
├── activeMQ/       # ActiveMQ (STOMP) implementations
├── grpc/           # gRPC (Protobuf) implementations
├── nats/           # NATS implementations
├── rabbitmq/       # RabbitMQ implementations
├── redis/          # Redis implementations
├── zeroMQ/         # ZeroMQ implementations
├── harness/        # Multi-receiver test harness
├── scripts/        # Utility scripts for data generation and table views
├── run_all_tests.py # Main test suite orchestrator
└── test_data.json  # Standardized test data
```

## 🛠 Setup & Installation

### Prerequisites

- **Python 3.8+**
- **C++ Compiler** (GCC/Clang with C++17 support)
- **CMake 3.10+**
- **Messaging Servers**: Ensure the respective servers are installed or available as binaries (the test harness attempts to start them automatically from `build` directories in some cases).

### Dependencies

#### Python
```bash
pip install redis pika nats-py grpcio grpcio-tools stomp.py pyzmq
```

#### C++
Each service directory contains a `cpp/` folder with its own build instructions or CMakeLists.txt. Common dependencies include `libhiredis`, `poblo-cpp`, `protobuf`, `grpc`, etc.

## 📈 Usage

### Running the Full Suite
To execute all supported test scenarios across all services and language combinations:

```bash
python3 run_all_tests.py
```

### Running Individual Tests
You can use the `test_harness.py` for granular control over a specific service:

```bash
python3 harness/test_harness.py --service redis --sender python --py-receivers 16 --cpp-receivers 16
```

## 📊 Performance Comparison

Latest benchmarking results (as shown in `walkthrough.md`):

---

## 📝 License
This project is provided for performance analysis and benchmarking purposes.
