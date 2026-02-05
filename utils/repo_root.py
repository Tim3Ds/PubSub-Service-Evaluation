#!/usr/bin/env python3
"""
Repository root utility module.
Provides a function to get the repository root directory using git.
"""
import os
import subprocess
from pathlib import Path


def get_repo_root() -> Path:
    """
    Get the repository root directory.
    
    Uses git rev-parse --show-toplevel to find the repository root.
    Falls back to the directory containing this file if git is not available.
    
    Returns:
        Path: The absolute path to the repository root directory.
    """
    # Get the directory where this module is located
    module_dir = Path(__file__).resolve().parent
    
    try:
        # Try to get repo root using git
        result = subprocess.run(
            ['git', 'rev-parse', '--show-toplevel'],
            cwd=module_dir,
            capture_output=True,
            text=True,
            timeout=5
        )
        
        if result.returncode == 0 and result.stdout.strip():
            return Path(result.stdout.strip())
    except (subprocess.SubprocessError, FileNotFoundError, OSError):
        # Git not available or command failed
        pass
    
    # Fallback: assume this module is in utils/ subdir of repo
    return module_dir.parent


# Create a singleton instance
_REPO_ROOT = None


def get_repo_root_cached() -> Path:
    """
    Get the repository root directory (cached).
    
    Uses git rev-parse --show-toplevel to find the repository root.
    Caches the result for subsequent calls.
    
    Returns:
        Path: The absolute path to the repository root directory.
    """
    global _REPO_ROOT
    if _REPO_ROOT is None:
        _REPO_ROOT = get_repo_root()
    return _REPO_ROOT


# Convenience function to get paths relative to repo root
def repo_path(*path_parts: str) -> Path:
    """
    Get a path relative to the repository root.
    
    Args:
        *path_parts: Path components to join with the repo root.
    
    Returns:
        Path: The absolute path to the specified location.
    """
    return get_repo_root_cached().joinpath(*path_parts)


if __name__ == '__main__':
    # When run directly, print the repo root path
    print(get_repo_root())

