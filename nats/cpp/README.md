# NATS C++ Examples

## Requirements

- C++ compiler (g++/clang++)
- CMake 3.10+
- Internet access (to fetch the NATS C client library)

## Build Instructions

From this directory:

```bash
make
```

- Executables will be placed in `build/bin/`.
- To clean build artifacts:
  ```bash
  make clean
  ```

## Usage

Start a NATS server (default port 4222):
```bash
nats-server
```

### Subscriber
Run the subscriber to listen on a subject (e.g., `test`):
```bash
./build/bin/subscriber nats://localhost:4222 test
```

### Publisher
Send a message to the same subject:
```bash
./build/bin/publisher nats://localhost:4222 test "your message"
```

- The server URL must be in the form `nats://host:port`.
