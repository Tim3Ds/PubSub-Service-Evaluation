# NATS Python Examples

## Requirements

- Python 3.7+
- Install the NATS Python client:
  ```bash
  pip install nats-py
  ```

## Usage

Start a NATS server (default port 4222):
```bash
nats-server
```

### Subscriber
Run the subscriber to listen on a subject (e.g., `test`):
```bash
./subscriber.py nats://localhost:4222 test
```

### Publisher
Send a message to the same subject:
```bash
./publisher.py nats://localhost:4222 test "your message"
```

- Make sure both scripts are executable (`chmod +x *.py`) or run them with `python3`.
- The server URL must be in the form `nats://host:port`.
