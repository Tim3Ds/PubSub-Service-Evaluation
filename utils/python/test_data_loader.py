#!/usr/bin/env python3
"""
Test Data Loader - Centralized library for loading test_data.json files.

This module provides a unified interface for loading test data across all
sender implementations, eliminating code duplication and providing consistent
path resolution.
"""
import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Any


# Default test data file name
DEFAULT_TEST_DATA_FILE = "test_data.json"


def get_default_test_data_path() -> Path:
    """
    Get the default path to test_data.json by searching common locations.
    
    Searches in the following order:
    1. Current working directory
    2. Parent directory (repo root)
    3. /home/tim/repos directory (absolute path)
    
    Returns:
        Path: The path to test_data.json
    """
    # List of potential locations to search
    search_paths = [
        Path.cwd(),                    # Current working directory
        Path.cwd().parent,             # Parent directory (repo root)
        Path("/home/tim/repos"),       # Absolute path
    ]
    
    for base_path in search_paths:
        test_data_path = base_path / DEFAULT_TEST_DATA_FILE
        if test_data_path.exists():
            return test_data_path
    
    # If not found in search paths, return default location in /home/tim/repos
    return Path("/home/tim/repos") / DEFAULT_TEST_DATA_FILE


def resolve_test_data_path(data_path: Optional[str] = None) -> Path:
    """
    Resolve the test data file path.
    
    Args:
        data_path: Optional custom path. If provided, uses this path.
                   If None, searches for default test_data.json.
    
    Returns:
        Path: The resolved path to test_data.json
    
    Raises:
        FileNotFoundError: If the file cannot be found at the specified or default location.
        PermissionError: If the file exists but cannot be read.
    """
    if data_path:
        # Use provided path
        path = Path(data_path)
        if not path.is_absolute():
            # If relative, make it relative to current working directory
            path = Path.cwd() / path
    else:
        # Use default path resolution
        path = get_default_test_data_path()
    
    # Verify the file exists and is readable
    if not path.exists():
        raise FileNotFoundError(f"test_data.json not found at: {path}")
    
    if not path.is_file():
        raise ValueError(f"Path is not a file: {path}")
    
    # Check read permissions
    if not os.access(path, os.R_OK):
        raise PermissionError(f"Cannot read file: {path}")
    
    return path


def load_test_data(data_path: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Load test data from a JSON file.
    
    This is the main function used by senders to load test data.
    It handles path resolution, file opening, and JSON parsing.
    
    Args:
        data_path: Optional path to the test data file. If not provided,
                   searches for test_data.json in common locations.
    
    Returns:
        List[Dict]: A list of message dictionaries loaded from the JSON file.
    
    Raises:
        FileNotFoundError: If the test data file cannot be found.
        PermissionError: If the file exists but cannot be read.
        json.JSONDecodeError: If the file contains invalid JSON.
        Exception: For any other errors during loading.
    
    Example:
        >>> from test_data_loader import load_test_data
        >>> test_data = load_test_data()  # Uses default location
        >>> # or
        >>> test_data = load_test_data("/path/to/test_data.json")
        >>> print(f"Loaded {len(test_data)} messages")
    """
    try:
        resolved_path = resolve_test_data_path(data_path)
        
        with open(resolved_path, 'r') as f:
            test_data = json.load(f)
        
        return test_data
    
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(
            f"Invalid JSON in test data file: {e.msg}",
            doc=e.doc,
            pos=e.pos
        )
    except Exception as e:
        raise Exception(f"Failed to load test data: {str(e)}")


def get_test_data_count(data_path: Optional[str] = None) -> int:
    """
    Get the number of messages in the test data file without loading all data.
    
    This is a lightweight function that just counts items in the JSON array
    without fully parsing the data structure.
    
    Args:
        data_path: Optional path to the test data file.
    
    Returns:
        int: Number of messages in the test data.
    
    Raises:
        FileNotFoundError: If the test data file cannot be found.
        PermissionError: If the file exists but cannot be read.
        json.JSONDecodeError: If the file contains invalid JSON.
    """
    resolved_path = resolve_test_data_path(data_path)
    
    with open(resolved_path, 'r') as f:
        # Read just enough to count the top-level array
        content = f.read()
        data = json.loads(content)
    
    return len(data)


def validate_test_data(test_data: List[Dict[str, Any]]) -> tuple[bool, List[str]]:
    """
    Validate test data structure.
    
    Checks that the test data has the expected structure with required fields.
    
    Args:
        test_data: The test data to validate.
    
    Returns:
        tuple: (is_valid, list_of_issues)
    """
    issues = []
    
    if not isinstance(test_data, list):
        issues.append("Test data must be a list of messages")
        return False, issues
    
    for i, msg in enumerate(test_data):
        if not isinstance(msg, dict):
            issues.append(f"Message {i} is not a dictionary")
            continue
        
        # Check for common required fields
        if 'message_id' not in msg:
            issues.append(f"Message {i} is missing 'message_id' field")
        
        if 'target' not in msg:
            issues.append(f"Message {i} is missing 'target' field")
        
        if 'payload' not in msg:
            issues.append(f"Message {i} is missing 'payload' field")
    
    return len(issues) == 0, issues


# Module initialization
if __name__ == "__main__":
    # Example usage and simple test
    import sys
    
    print("Test Data Loader - Simple Test")
    print("=" * 40)
    
    try:
        # Try to load test data
        test_data = load_test_data()
        print(f"✓ Successfully loaded {len(test_data)} messages")
        
        # Validate structure
        is_valid, issues = validate_test_data(test_data)
        if is_valid:
            print("✓ Test data structure is valid")
        else:
            print(f"✗ Validation issues found:")
            for issue in issues:
                print(f"  - {issue}")
        
        # Show sample message
        if test_data:
            print(f"\nSample message (first item):")
            print(json.dumps(test_data[0], indent=2))
    
    except FileNotFoundError as e:
        print(f"✗ File not found: {e}")
        sys.exit(1)
    except PermissionError as e:
        print(f"✗ Permission denied: {e}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"✗ Invalid JSON: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"✗ Error: {e}")
        sys.exit(1)

