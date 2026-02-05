#!/usr/bin/env python3
"""
Centralized path constants for the project.
Uses repo_root.py to get the repository root and defines standard paths.
"""
from pathlib import Path
from repo_root import get_repo_root_cached


# Get repository root
REPO_ROOT = get_repo_root_cached()

# Standard directory paths (relative to repo root)
REPO_ROOT_PATH = REPO_ROOT
UTILS_PATH = REPO_ROOT / 'utils'
UTILS_PYTHON_PATH = UTILS_PATH / 'python'
UTILS_CPP_PATH = UTILS_PATH / 'cpp'

# Service paths
REDIS_PATH = REPO_ROOT / 'redis'
REDIS_PYTHON_PATH = REDIS_PATH / 'python'
REDIS_CPP_PATH = REDIS_PATH / 'cpp'

RABBITMQ_PATH = REPO_ROOT / 'rabbitmq'
RABBITMQ_PYTHON_PATH = RABBITMQ_PATH / 'python'

ZEROMQ_PATH = REPO_ROOT / 'zeroMQ'
ZEROMQ_PYTHON_PATH = ZEROMQ_PATH / 'python'
ZEROMQ_CPP_PATH = ZEROMQ_PATH / 'cpp'

NATS_PATH = REPO_ROOT / 'nats'
NATS_PYTHON_PATH = NATS_PATH / 'python'

GRPC_PATH = REPO_ROOT / 'grpc'
GRPC_PYTHON_PATH = GRPC_PATH / 'python'
GRPC_CPP_PATH = GRPC_PATH / 'cpp'

ACTIVEMQ_PATH = REPO_ROOT / 'activeMQ'
ACTIVEMQ_PYTHON_PATH = ACTIVEMQ_PATH / 'python-client'

# Build paths
GRPC_CPP_BUILD_PATH = GRPC_CPP_PATH / 'build'
GRPC_CPP_DEPS_PATH = GRPC_CPP_BUILD_PATH / 'deps'

# Test data path
TEST_DATA_PATH = REPO_ROOT / 'test_data.json'

# Report path (in current working directory)
REPORT_PATH = Path.cwd() / 'logs/report.txt'


def get_grpc_python_path() -> Path:
    """Get the gRPC Python path."""
    return GRPC_PYTHON_PATH


def get_utils_python_path() -> Path:
    """Get the utils/python path."""
    return UTILS_PYTHON_PATH


def get_service_python_path(service: str) -> Path:
    """
    Get the Python path for a service.
    
    Args:
        service: Service name (redis, rabbitmq, zeromq, nats, grpc, activemq)
    
    Returns:
        Path: The service's Python directory.
    """
    service_paths = {
        'redis': REDIS_PYTHON_PATH,
        'rabbitmq': RABBITMQ_PYTHON_PATH,
        'zeromq': ZEROMQ_PYTHON_PATH,
        'nats': NATS_PYTHON_PATH,
        'grpc': GRPC_PYTHON_PATH,
        'activemq': ACTIVEMQ_PYTHON_PATH,
    }
    return service_paths.get(service.lower())


if __name__ == '__main__':
    # Print all paths when run directly
    print(f"REPO_ROOT: {REPO_ROOT}")
    print(f"UTILS_PATH: {UTILS_PATH}")
    print(f"UTILS_PYTHON_PATH: {UTILS_PYTHON_PATH}")
    print(f"TEST_DATA_PATH: {TEST_DATA_PATH}")

