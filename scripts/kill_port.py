#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cross-platform script to find and kill processes using a specific port

Supports Windows, macOS, and Linux
"""

import sys
import platform
import subprocess
import argparse


def find_process_using_port_windows(port):
    """Find process using a port on Windows using netstat"""
    try:
        # Use netstat to find process
        cmd = f"netstat -ano | findstr :{port}"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

        if result.returncode != 0 or not result.stdout.strip():
            return []

        pids = set()
        for line in result.stdout.strip().split('\n'):
            parts = line.split()
            if len(parts) >= 5:
                # Last column is PID
                pid = parts[-1]
                if pid.isdigit():
                    pids.add(int(pid))

        return list(pids)
    except Exception as e:
        print(f"Error finding process on Windows: {e}")
        return []


def find_process_using_port_unix(port):
    """Find process using a port on Unix-like systems (macOS, Linux) using lsof"""
    try:
        # Use lsof to find process
        cmd = ["lsof", "-ti", f":{port}"]
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode != 0 or not result.stdout.strip():
            return []

        pids = []
        for line in result.stdout.strip().split('\n'):
            if line.strip().isdigit():
                pids.append(int(line.strip()))

        return pids
    except FileNotFoundError:
        print("Error: 'lsof' command not found. Please install it.")
        return []
    except Exception as e:
        print(f"Error finding process on Unix: {e}")
        return []


def get_process_info_windows(pid):
    """Get process information on Windows"""
    try:
        cmd = f'tasklist /FI "PID eq {pid}" /FO CSV /NH'
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)

        if result.returncode == 0 and result.stdout.strip():
            # Parse CSV output: "process_name","pid","session","mem","status"
            parts = result.stdout.strip().strip('"').split('","')
            if len(parts) >= 1:
                return parts[0]
        return f"PID {pid}"
    except:
        return f"PID {pid}"


def get_process_info_unix(pid):
    """Get process information on Unix-like systems"""
    try:
        cmd = ["ps", "-p", str(pid), "-o", "comm="]
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
        return f"PID {pid}"
    except:
        return f"PID {pid}"


def kill_process_windows(pid, force=False):
    """Kill process on Windows"""
    try:
        if force:
            cmd = f"taskkill /F /PID {pid}"
        else:
            cmd = f"taskkill /PID {pid}"

        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        return result.returncode == 0
    except Exception as e:
        print(f"Error killing process on Windows: {e}")
        return False


def kill_process_unix(pid, force=False):
    """Kill process on Unix-like systems"""
    try:
        import signal
        if force:
            cmd = ["kill", "-9", str(pid)]
        else:
            cmd = ["kill", str(pid)]

        result = subprocess.run(cmd, capture_output=True, text=True)
        return result.returncode == 0
    except Exception as e:
        print(f"Error killing process on Unix: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Find and kill processes using a specific port (cross-platform)"
    )
    parser.add_argument(
        "port",
        type=int,
        help="Port number to check"
    )
    parser.add_argument(
        "-f", "--force",
        action="store_true",
        help="Force kill processes"
    )
    parser.add_argument(
        "-y", "--yes",
        action="store_true",
        help="Skip confirmation prompt"
    )

    args = parser.parse_args()
    port = args.port

    system = platform.system()
    print(f"Checking port {port} on {system}...")

    # Find processes using the port
    if system == "Windows":
        pids = find_process_using_port_windows(port)
        get_info = get_process_info_windows
        kill_func = kill_process_windows
    else:  # macOS, Linux
        pids = find_process_using_port_unix(port)
        get_info = get_process_info_unix
        kill_func = kill_process_unix

    if not pids:
        print(f"✅ Port {port} is free")
        return 0

    print(f"\nFound {len(pids)} process(es) using port {port}:")
    for pid in pids:
        process_name = get_info(pid)
        print(f"  - {process_name} (PID: {pid})")

    # Ask for confirmation unless -y flag is set
    if not args.yes:
        response = input(f"\nKill {'all' if len(pids) > 1 else 'this'} process(es)? (y/N): ")
        if response.lower() != 'y':
            print("Cancelled")
            return 1

    # Kill processes
    killed_count = 0
    for pid in pids:
        process_name = get_info(pid)
        if kill_func(pid, force=args.force):
            print(f"✅ Killed {process_name} (PID: {pid})")
            killed_count += 1
        else:
            print(f"❌ Failed to kill {process_name} (PID: {pid})")

    if killed_count == len(pids):
        print(f"\n✅ Successfully killed all processes on port {port}")
        return 0
    elif killed_count > 0:
        print(f"\n⚠️  Killed {killed_count}/{len(pids)} processes")
        return 1
    else:
        print(f"\n❌ Failed to kill any processes")
        if not args.force:
            print("Try using --force flag")
        return 1


if __name__ == "__main__":
    sys.exit(main())
