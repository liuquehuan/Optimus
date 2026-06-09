#!/usr/bin/env python3

import subprocess
import sys
import os
import time
from datetime import datetime

def run_mode(scan_mode: str, script_path: str = "run_alloydb.py"):
    print(f"\n{'='*80}")
    print(f"Starting mode: {scan_mode.upper()}")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}\n")
    
    cmd = [
        sys.executable,
        script_path,
        "--db", "100x",
        "--scan-mode", scan_mode
    ]
    
    try:
        script_dir = os.path.dirname(os.path.abspath(script_path))
        if not script_dir:
            script_dir = os.getcwd()
        
        result = subprocess.run(
            cmd,
            check=True,
            cwd=script_dir,
        )
        print(f"\n{'='*80}")
        print(f"Mode {scan_mode.upper()} completed")
        print(f"Completion time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*80}\n")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n{'='*80}")
        print(f"Error: mode {scan_mode.upper()} failed")
        print(f"Return code: {e.returncode}")
        print(f"{'='*80}\n")
        return False
    except KeyboardInterrupt:
        print(f"\n\n{'='*80}")
        print(f"Interrupted by user: mode {scan_mode.upper()} was cancelled")
        print(f"{'='*80}\n")
        return False


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    script_path = os.path.join(script_dir, "run_alloydb.py")
    
    if not os.path.exists(script_path):
        print(f"Error: script file not found: {script_path}")
        sys.exit(1)
    
    scan_modes = ["row", "column", "mixed"]
    
    overall_start_time = time.time()
    print(f"\n{'='*80}")
    print("Starting hybench 100x benchmark for all modes")
    print(f"Overall start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Modes: {', '.join(scan_modes)}")
    print(f"{'='*80}\n")
    
    results = {}
    
    for i, mode in enumerate(scan_modes, 1):
        mode_start_time = time.time()
        print(f"\nProgress: [{i}/{len(scan_modes)}] preparing mode: {mode}")
        
        success = run_mode(mode, script_path)
        mode_elapsed = time.time() - mode_start_time
        
        results[mode] = {
            'success': success,
            'elapsed_time': mode_elapsed
        }
        
        if not success:
            print(f"Warning: mode {mode} failed, continuing with the next mode...")
        
        if i < len(scan_modes):
            print("\nWaiting 5 seconds before the next mode...\n")
            time.sleep(5)
    
    overall_elapsed = time.time() - overall_start_time
    print(f"\n{'='*80}")
    print("All modes completed")
    print(f"Overall end time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total elapsed time: {overall_elapsed / 3600:.2f} hours ({overall_elapsed / 60:.2f} minutes)")
    print("\nRun summary:")
    print(f"{'='*80}")
    for mode, result in results.items():
        status = "✓ success" if result['success'] else "✗ failed"
        elapsed_hours = result['elapsed_time'] / 3600
        elapsed_mins = result['elapsed_time'] / 60
        print(f"  {mode:8s}: {status:9s} | elapsed: {elapsed_hours:6.2f} hours ({elapsed_mins:6.2f} minutes)")
    print(f"{'='*80}\n")
    
    if not all(r['success'] for r in results.values()):
        print("Warning: some modes failed")
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user: program cancelled")
        sys.exit(130)

