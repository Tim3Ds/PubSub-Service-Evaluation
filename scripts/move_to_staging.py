#!/usr/bin/env python3
"""Move all paths listed in delete_prep.txt files to a staging folder.

This preserves directory structure relative to each repo, so files can be
easily recovered if needed before permanent deletion.

Usage: python scripts/move_to_staging.py
"""
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
STAGING = ROOT / 'DELETE_STAGING'

def move_paths_from_file(delete_file: Path, base_repo: Path):
    """Move all paths in delete_file to staging, relative to base_repo."""
    moved = 0
    failed = 0
    
    if not delete_file.exists():
        return moved, failed
    
    with delete_file.open('r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            
            src = base_repo / line
            if not src.exists():
                print(f"  SKIP (not found): {line}")
                failed += 1
                continue
            
            # Preserve repo structure in staging
            rel_to_root = base_repo.relative_to(ROOT)
            dest = STAGING / rel_to_root / line
            dest.parent.mkdir(parents=True, exist_ok=True)
            
            try:
                if src.is_dir():
                    shutil.move(str(src), str(dest))
                else:
                    shutil.move(str(src), str(dest))
                print(f"  MOVED: {line}")
                moved += 1
            except Exception as e:
                print(f"  ERROR: {line} - {e}")
                failed += 1
    
    return moved, failed

def main():
    print(f"Creating staging folder at {STAGING}")
    STAGING.mkdir(exist_ok=True)
    
    print(f"\nScanning for delete_prep.txt files under {ROOT}")
    repos = sorted([p for p in ROOT.iterdir() if p.is_dir() and p.name not in ('.git', 'scripts')])
    
    total_moved = 0
    total_failed = 0
    
    for repo in repos:
        delete_file = repo / 'delete_prep.txt'
        if not delete_file.exists():
            continue
        
        print(f"\nProcessing {repo.name}/delete_prep.txt:")
        moved, failed = move_paths_from_file(delete_file, repo)
        total_moved += moved
        total_failed += failed
        print(f"  → {moved} moved, {failed} failed/skipped")
    
    # Process root delete_prep.txt
    root_delete_file = ROOT / 'delete_prep.txt'
    if root_delete_file.exists():
        print(f"\nProcessing root delete_prep.txt:")
        moved, failed = move_paths_from_file(root_delete_file, ROOT)
        total_moved += moved
        total_failed += failed
        print(f"  → {moved} moved, {failed} failed/skipped")
    
    print(f"\n{'='*60}")
    print(f"STAGING COMPLETE")
    print(f"Total moved: {total_moved}")
    print(f"Total failed/skipped: {total_failed}")
    print(f"Staging folder: {STAGING}")
    print(f"{'='*60}")

if __name__ == '__main__':
    main()
