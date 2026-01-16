#!/usr/bin/env python3
"""Scan subdirectories for common build/generated artifacts and create
per-repo delete_prep.txt files listing paths to remove.

Usage: python scripts/generate_delete_prep.py
"""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# Patterns to look for (names or glob patterns relative to each repo root)
DIR_NAMES = [
    'build', 'build-*', 'cmake-build-*', '_deps', 'deps', 'bin', 'lib', 'CMakeFiles',
    '__pycache__', '.venv', 'venv', 'env', 'build', 'tmp'
]

FILE_GLOBS = [
    '*.o', '*.obj', '*.so', '*.a', '*.lib', '*.dll', '*.dylib', '*.exe', '*.out',
    '*.log', '*.tmp', '*.pid', '*.pyc', '*_pb.h', '*_pb.cc', '*_grpc.pb.h', '*_grpc.pb.cc'
]

def scan_repo(repo_path: Path):
    found = []
    # Check directories by name
    for name in DIR_NAMES:
        # support simple glob like build-*
        for p in repo_path.glob(name):
            found.append(p)
        # also search recursively for matching dir name
        for p in repo_path.rglob(name):
            found.append(p)

    # Check file globs recursively
    for pattern in FILE_GLOBS:
        for p in repo_path.rglob(pattern):
            found.append(p)

    # dedupe and sort
    found = sorted({p.resolve() for p in found})
    return found

def main():
    print(f"Scanning repos under {ROOT}")
    entries = [p for p in ROOT.iterdir() if p.is_dir()]
    total = 0
    for repo in entries:
        # skip scripts and the root .gitignore location
        if repo.name in ('.git', 'scripts'):
            continue
        found = scan_repo(repo)
        if not found:
            continue
        out_file = repo / 'delete_prep.txt'
        with out_file.open('w') as f:
            for p in found:
                # write paths relative to repo root when possible
                try:
                    rel = p.relative_to(repo)
                except Exception:
                    rel = p
                f.write(str(rel) + '\n')
        print(f"Wrote {len(found)} entries to {out_file}")
        total += len(found)

    # Also create a root-level delete_prep.txt for matches in root
    found_root = scan_repo(ROOT)
    if found_root:
        out_file = ROOT / 'delete_prep.txt'
        with out_file.open('w') as f:
            for p in found_root:
                try:
                    rel = p.relative_to(ROOT)
                except Exception:
                    rel = p
                f.write(str(rel) + '\n')
        print(f"Wrote {len(found_root)} entries to {out_file}")
        total += len(found_root)

    print(f"Scan complete, {total} candidate paths found.")

if __name__ == '__main__':
    main()
