#!/usr/bin/env python3
"""
Rebuild All Services Script

This script rebuilds all C++ services and installs Python dependencies
for the messaging services project. It generates a detailed JSON report
that can be used to diagnose and fix build problems.

Services covered:
- C++: grpc, activeMQ, nats, rabbitmq, redis, zeroMQ
- Python: grpc, activeMQ, nats, rabbitmq, redis, zeroMQ

Usage:
    python3 rebuild_all_services.py [--clean] [--skip-cpp] [--skip-python] [--verbose]

Options:
    --clean      Clean build directories before rebuilding
    --skip-cpp   Skip C++ builds
    --skip-python Skip Python dependency installation
    --verbose    Enable verbose output
"""

import subprocess
import sys
import os
import json
import time
import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any


class BuildReport:
    """Handles generation of the build report."""
    
    def __init__(self):
        self.report = {
            "metadata": {
                "timestamp": datetime.now().isoformat(),
                "hostname": os.uname().nodename if hasattr(os, 'uname') else "unknown",
                "cwd": os.getcwd(),
                "python_version": sys.version,
            },
            "services": [],
            "summary": {
                "total_services": 0,
                "successful": 0,
                "failed": 0,
                "start_time": None,
                "end_time": None,
                "duration_seconds": 0
            }
        }
        self.current_service = None
    
    def start_service(self, name: str, build_type: str, command: str, path: str):
        """Start recording a service build."""
        self.current_service = {
            "name": name,
            "build_type": build_type,
            "command": command,
            "path": path,
            "status": "running",
            "start_time": datetime.now().isoformat(),
            "output": [],
            "executables": [],
            "errors": [],
            "warnings": [],
            "commands_executed": []
        }
        self.report["services"].append(self.current_service)
        self.report["summary"]["total_services"] += 1
    
    def add_output(self, text: str):
        """Add output to current service."""
        if self.current_service:
            self.current_service["output"].append({
                "timestamp": datetime.now().isoformat(),
                "text": text
            })
    
    def add_warning(self, warning: str):
        """Add a warning to current service."""
        if self.current_service:
            self.current_service["warnings"].append({
                "timestamp": datetime.now().isoformat(),
                "message": warning
            })
    
    def add_error(self, error: str):
        """Add an error to current service."""
        if self.current_service:
            self.current_service["errors"].append({
                "timestamp": datetime.now().isoformat(),
                "message": error
            })
    
    def set_executables(self, executables: List[str]):
        """Set the list of executables created."""
        if self.current_service:
            self.current_service["executables"] = executables
    
    def complete_service(self, success: bool, exit_code: int = 0):
        """Mark the current service as completed."""
        if self.current_service:
            self.current_service["status"] = "success" if success else "failed"
            self.current_service["exit_code"] = exit_code
            self.current_service["end_time"] = datetime.now().isoformat()
            
            # Calculate duration
            if self.current_service.get("start_time") and self.current_service.get("end_time"):
                start = datetime.fromisoformat(self.current_service["start_time"])
                end = datetime.fromisoformat(self.current_service["end_time"])
                self.current_service["duration_seconds"] = (end - start).total_seconds()
            
            if success:
                self.report["summary"]["successful"] += 1
            else:
                self.report["summary"]["failed"] += 1
    
    def save(self, filename: Optional[str] = None):
        """Save the report to a JSON file."""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            filename = f"logs/build_report_{timestamp}.json"
        
        self.report["summary"]["end_time"] = datetime.now().isoformat()
        
        if self.report["summary"]["start_time"] and self.report["summary"]["end_time"]:
            start = datetime.fromisoformat(self.report["summary"]["start_time"])
            end = datetime.fromisoformat(self.report["summary"]["end_time"])
            self.report["summary"]["duration_seconds"] = (end - start).total_seconds()
        
        with open(filename, 'w') as f:
            json.dump(self.report, f, indent=2)
        
        return filename


class ServiceBuilder:
    """Builds services and records results."""
    
    def __init__(self, report: BuildReport, verbose: bool = False):
        self.report = report
        self.verbose = verbose
    
    def run_command(self, cmd: str, cwd: str, timeout: int = 3600, stream: bool = True) -> tuple:
        """
        Run a command and return the result.
        
        Args:
            cmd: Command to run
            cwd: Working directory
            timeout: Timeout in seconds
            stream: Whether to stream output in real-time
            
        Returns: (success: bool, exit_code: int, stdout: str, stderr: str)
        """
        self.report.add_output(f"Running command: {cmd}")
        self.report.add_output(f"Working directory: {cwd}")
        # record the command executed for this service
        if getattr(self.report, 'current_service', None) is not None and self.report.current_service is not None:
            self.report.current_service.setdefault("commands_executed", []).append({
                "timestamp": datetime.now().isoformat(),
                "command": cmd,
                "cwd": cwd
            })
        
        try:
            process = subprocess.Popen(
                cmd,
                shell=True,
                cwd=cwd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1  # Line buffered
            )
            
            stdout = ""
            
            if stream:
                # Stream output in real-time
                while True:
                    line = process.stdout.readline()
                    if line:
                        stdout += line
                        self.report.add_output(line.rstrip('\n'))
                        if self.verbose:
                            print(line, end='')
                    elif process.poll() is not None:
                        break
            else:
                stdout, _ = process.communicate(timeout=timeout)
                for line in stdout.strip().split('\n'):
                    if line:
                        self.report.add_output(f"[stdout] {line}")
            
            exit_code = process.wait()
            success = exit_code == 0
            if not success:
                # capture last part of stdout as an error message
                sample = stdout[-400:] if len(stdout) > 400 else stdout
                self.report.add_error(f"Command exited with code {exit_code}: {cmd}\n{sample}")
            return success, exit_code, stdout, ""
            
        except subprocess.TimeoutExpired:
            self.report.add_error(f"Command timed out after {timeout} seconds")
            return False, -1, "", "Timeout expired"
        except Exception as e:
            self.report.add_error(f"Exception occurred: {str(e)}")
            return False, -1, "", str(e)
    
    def find_executables(self, build_dir: str, patterns: List[str] = None) -> List[str]:
        """Find executables in the build directory."""
        executables = []
        build_path = Path(build_dir)
        
        if not build_path.exists():
            return executables
        
        # Common test executable names
        test_names = ['sender_test', 'receiver_test', 'sender_async_test', 'receiver_async_test']
        
        for pattern in (patterns or test_names):
            for exe in build_path.rglob(pattern):
                if exe.is_file() and os.access(exe, os.X_OK):
                    executables.append(str(exe))
        
        return executables
    
    def build_cpp_service(self, name: str, path: str, build_cmd: str = "make build") -> bool:
        """Build a C++ service."""
        self.report.start_service(name, "C++", build_cmd, path)
        
        success, exit_code, stdout, stderr = self.run_command(build_cmd, path)
        
        if success:
            # Look for executables
            build_dir = os.path.join(path, "build", "bin")
            executables = self.find_executables(build_dir)
            
            if executables:
                self.report.set_executables(executables)
                self.report.add_output(f"Found {len(executables)} executables")
            else:
                self.report.add_warning("No executables found in build/bin directory")
        
        self.report.complete_service(success, exit_code)
        return success
    
    def install_python_deps(self, name: str, path: str, deps: List[str]) -> bool:
        """Install Python dependencies for a service."""
        # Use python3 -m pip for better cross-platform compatibility
        cmd = f"python3 -m pip install {' '.join(deps)}"
        self.report.start_service(name, "Python", cmd, path)
        
        success, exit_code, stdout, stderr = self.run_command(cmd, path)
        
        self.report.complete_service(success, exit_code)
        return success


def get_cpp_services() -> List[Dict[str, str]]:
    """Return list of C++ services to build."""
    return [
        {
            "name": "grpc/cpp",
            "path": "grpc/cpp",
            "build_cmd": "make build",
            "description": "gRPC C++ service with protobuf"
        },
        {
            "name": "activeMQ/cpp-client",
            "path": "activeMQ/cpp-client",
            "build_cmd": "make build",
            "description": "ActiveMQ C++ client"
        },
        {
            "name": "nats/cpp",
            "path": "nats/cpp",
            "build_cmd": "make build",
            "description": "NATS C++ client"
        },
        {
            "name": "rabbitmq/cpp",
            "path": "rabbitmq/cpp",
            "build_cmd": "make build",
            "description": "RabbitMQ C++ client"
        },
        {
            "name": "redis/cpp",
            "path": "redis/cpp",
            "build_cmd": "make build",
            "description": "Redis C++ client"
        },
        {
            "name": "zeroMQ/cpp",
            "path": "zeroMQ/cpp",
            "build_cmd": "make build",
            "description": "ZeroMQ C++ client"
        }
    ]


def get_python_services() -> List[Dict[str, Any]]:
    """Return list of Python services with their dependencies."""
    return [
        {
            "name": "grpc/python",
            "path": "grpc/python",
            "deps": ["grpcio", "grpcio-tools"],
            "requirements_file": "grpc/python/requirements.txt"
        },
        {
            "name": "activeMQ/python-client",
            "path": "activeMQ/python-client",
            "deps": ["stomp.py>=8.0.0"],
            "requirements_file": "activeMQ/python-client/requirements.txt"
        },
        {
            "name": "nats/python",
            "path": "nats/python",
            "deps": ["nats-py"],
            "requirements_file": None
        },
        {
            "name": "rabbitmq/python",
            "path": "rabbitmq/python",
            "deps": ["pika", "aio_pika"],
            "requirements_file": None
        },
        {
            "name": "redis/python",
            "path": "redis/python",
            "deps": ["redis"],
            "requirements_file": None
        },
        {
            "name": "zeroMQ/python",
            "path": "zeroMQ/python",
            "deps": ["pyzmq"],
            "requirements_file": None
        }
    ]


def clean_build_dirs(services: List[Dict]) -> None:
    """Clean build directories for given services."""
    print("\n[INFO] Cleaning build directories...")
    for service in services:
        path = service.get("path", "")
        build_dir = os.path.join(path, "build")
        if os.path.exists(build_dir):
            print(f"  - Removing {build_dir}")
            import shutil
            shutil.rmtree(build_dir)


def print_summary(report: BuildReport) -> None:
    """Print a summary of the build results."""
    print("\n" + "=" * 70)
    print("BUILD SUMMARY")
    print("=" * 70)
    
    summary = report.report["summary"]
    print(f"Total services: {summary['total_services']}")
    print(f"Successful: {summary['successful']}")
    print(f"Failed: {summary['failed']}")
    
    if summary.get("duration_seconds"):
        print(f"Total time: {summary['duration_seconds']:.2f} seconds")
    
    print("\nService Details:")
    print("-" * 70)
    
    for service in report.report["services"]:
        status_icon = "✓" if service["status"] == "success" else "✗"
        print(f"{status_icon} {service['name']:30} [{service['build_type']:6}] - {service['status']}")
        
        if service["status"] == "failed" and service.get("errors"):
            print(f"    Errors: {len(service['errors'])}")
            for error in service["errors"][:3]:  # Show first 3 errors
                print(f"      - {error['message'][:100]}")
        
        if service.get("executables"):
            print(f"    Executables: {len(service['executables'])}")
            for exe in service["executables"]:
                print(f"      - {os.path.basename(exe)}")
    
    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description="Rebuild all C++ services and install Python dependencies"
    )
    parser.add_argument("--clean", action="store_true",
                        help="Clean build directories before rebuilding")
    parser.add_argument("--skip-cpp", action="store_true",
                        help="Skip C++ builds")
    parser.add_argument("--skip-python", action="store_true",
                        help="Skip Python dependency installation")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Enable verbose output")
    
    args = parser.parse_args()
    
    # Initialize report
    report = BuildReport()
    report.report["summary"]["start_time"] = datetime.now().isoformat()
    
    builder = ServiceBuilder(report, args.verbose)
    
    cpp_services = get_cpp_services()
    python_services = get_python_services()
    
    # Clean if requested
    if args.clean:
        clean_build_dirs(cpp_services)
    
    total_services = 0
    completed = 0
    failed = 0
    
    # Build C++ services
    if not args.skip_cpp:
        print("\n[INFO] Building C++ services...")
        print("-" * 50)
        
        for service in cpp_services:
            total_services += 1
            print(f"\n[Building] {service['name']}...")
            print(f"  Path: {service['path']}")
            print(f"  Command: {service['build_cmd']}")
            print("-" * 50)
            
            success = builder.build_cpp_service(
                service['name'],
                service['path'],
                service['build_cmd']
            )
            
            if success:
                completed += 1
                print(f"  ✓ {service['name']} built successfully")
            else:
                failed += 1
                print(f"  ✗ {service['name']} build failed")
    
    # Install Python dependencies
    if not args.skip_python:
        print("\n[INFO] Installing Python dependencies...")
        print("-" * 50)
        
        for service in python_services:
            total_services += 1
            
            # Check if requirements file exists and use it
            req_file = service.get("requirements_file")
            if req_file and os.path.exists(req_file):
                cmd = f"python3 -m pip install -r {req_file}"
                deps_str = f"(from {req_file})"
            else:
                cmd = f"python3 -m pip install {' '.join(service['deps'])}"
                deps_str = f"({', '.join(service['deps'])})"
            
            print(f"\n[Installing] {service['name']} {deps_str}...")
            print(f"  Path: {service['path']}")
            print(f"  Command: {cmd}")
            print("-" * 50)
            
            success = builder.install_python_deps(
                service['name'],
                service['path'],
                service['deps']
            )
            
            if success:
                completed += 1
                print(f"  ✓ {service['name']} dependencies installed")
            else:
                failed += 1
                print(f"  ✗ {service['name']} dependency installation failed")
    
    # Save report
    report_filename = report.save()
    print(f"\n[INFO] Build report saved to: {report_filename}")
    
    # Print summary
    print_summary(report)
    
    # Exit with appropriate code
    if failed > 0:
        print(f"\n[WARN] {failed} service(s) failed to build/install")
        print(f"       Review the report at {report_filename} for details")
        sys.exit(1)
    else:
        print(f"\n[SUCCESS] All {completed} services built successfully!")
        sys.exit(0)


if __name__ == "__main__":
    main()

