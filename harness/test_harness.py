#!/usr/bin/env python3
"""
Multi-Receiver Test Harness
Spawns and monitors multiple receiver processes with configurable Python/C++ split.
"""

import subprocess
import sys
import os
import json
import time
import argparse
import signal
from pathlib import Path

class TestHarness:
    def __init__(self, service: str, sender_lang: str, py_receivers: int, cpp_receivers: int):
        self.service = service
        self.sender_lang = sender_lang
        self.py_receivers = py_receivers
        self.cpp_receivers = cpp_receivers
        self.total_receivers = py_receivers + cpp_receivers
        self.receiver_procs = []
        self.sender_proc = None
        self.base_dir = Path(__file__).parent.parent
        
        # Service directory mapping
        self.service_dirs = {
            'redis': 'redis',
            'rabbitmq': 'rabbitmq',
            'zeromq': 'zeroMQ',
            'nats': 'nats',
            'grpc': 'grpc',
            'activemq': 'activeMQ'
        }
        
    def get_service_path(self) -> Path:
        return self.base_dir / self.service_dirs[self.service]
    
    def get_receiver_cmd(self, lang: str, receiver_id: int) -> list:
        service_path = self.get_service_path()
        prefix = 'python' if lang == 'python' else 'cpp'
        if self.service == 'activemq':
            prefix = 'python-client' if lang == 'python' else 'cpp-client'
        
        if lang == 'python':
            return ['python3', str(service_path / prefix / 'receiver_test.py'), '--id', str(receiver_id)]
        else:  # cpp
            return [str(service_path / prefix / 'build' / 'bin' / 'receiver_test'), '--id', str(receiver_id)]
    
    def get_sender_cmd(self) -> list:
        service_path = self.get_service_path()
        prefix = 'python' if self.sender_lang == 'python' else 'cpp'
        if self.service == 'activemq':
            prefix = 'python-client' if self.sender_lang == 'python' else 'cpp-client'
            
        if self.sender_lang == 'python':
            return ['python3', str(service_path / prefix / 'sender_test.py')]
        else:  # cpp
            return [str(service_path / prefix / 'build' / 'bin' / 'sender_test')]
    
    def spawn_receivers(self):
        print(f"[Harness] Spawning {self.total_receivers} receivers ({self.py_receivers} Python, {self.cpp_receivers} C++)...")
        
        receiver_id = 0
        
        # Spawn Python receivers
        for i in range(self.py_receivers):
            cmd = self.get_receiver_cmd('python', receiver_id)
            log_file = open(f'/tmp/receiver_{receiver_id}.log', 'w')
            proc = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT)
            self.receiver_procs.append((proc, log_file, receiver_id, 'python'))
            print(f"  [+] Receiver {receiver_id} (Python) started, PID={proc.pid}")
            receiver_id += 1
        
        # Spawn C++ receivers
        for i in range(self.cpp_receivers):
            cmd = self.get_receiver_cmd('cpp', receiver_id)
            log_file = open(f'/tmp/receiver_{receiver_id}.log', 'w')
            proc = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT)
            self.receiver_procs.append((proc, log_file, receiver_id, 'cpp'))
            print(f"  [+] Receiver {receiver_id} (C++) started, PID={proc.pid}")
            receiver_id += 1
        
        # Give receivers time to start
        time.sleep(2)
    
    def run_sender(self):
        print(f"[Harness] Starting sender ({self.sender_lang})...")
        cmd = self.get_sender_cmd()
        self.sender_proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        
        # Stream sender output
        for line in self.sender_proc.stdout:
            print(f"  [Sender] {line.rstrip()}")
        
        self.sender_proc.wait()
        print(f"[Harness] Sender finished with exit code {self.sender_proc.returncode}")
    
    def stop_receivers(self):
        print("[Harness] Stopping receivers...")
        for proc, log_file, receiver_id, lang in self.receiver_procs:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
            log_file.close()
            print(f"  [-] Receiver {receiver_id} ({lang}) stopped")
    
    def aggregate_results(self) -> dict:
        print("[Harness] Aggregating results...")
        results = {
            'service': self.service,
            'sender_lang': self.sender_lang,
            'py_receivers': self.py_receivers,
            'cpp_receivers': self.cpp_receivers,
            'receiver_stats': []
        }
        
        for _, _, receiver_id, lang in self.receiver_procs:
            log_path = f'/tmp/receiver_{receiver_id}.log'
            if os.path.exists(log_path):
                with open(log_path, 'r') as f:
                    content = f.read()
                    results['receiver_stats'].append({
                        'id': receiver_id,
                        'lang': lang,
                        'log_size': len(content)
                    })
        
        return results
    
    def start_server(self):
        print(f"[Harness] Starting server/broker/router for {self.service}...")
        service_path = self.get_service_path()
        
        self.server_proc = None
        cmd = []
        
        if self.service == 'redis':
            cmd = [str(service_path / 'build' / 'redis-6.2.6' / 'src' / 'redis-server')]
        elif self.service == 'rabbitmq':
            cmd = [str(service_path / 'build' / 'rabbitmq_server-3.7.28' / 'sbin' / 'rabbitmq-server')]
            # RabbitMQ can be slow to start, especially when running multiple tests in sequence
            time.sleep(15)
        elif self.service == 'nats':
            cmd = [str(service_path / 'build' / 'bin' / 'nats-server')]
        elif self.service == 'activemq':
            # ActiveMQ has its own start/stop script
            # 6.2.0 requires Java 17
            cmd = [str(service_path / 'apache-activemq-6.2.0' / 'bin' / 'activemq'), 'start']
            env = os.environ.copy()
            # Use Java 17 if available
            java17_home = '/usr/lib/jvm/java-17-openjdk-17.0.17.0.10-1.el8.x86_64'
            if os.path.isdir(java17_home):
                env['JAVA_HOME'] = java17_home
            
            subprocess.run(cmd, check=True, env=env)
            time.sleep(15) # Give it more time to start
            return
        elif self.service == 'zeromq':
            cmd = ['python3', '-u', str(service_path / 'python' / 'router.py')]
        elif self.service == 'grpc':
            # Only start central server if it exists and is intended for this test
            server_script = service_path / 'python' / 'server.py'
            if server_script.exists():
                cmd = ['python3', '-u', str(server_script)]
            else:
                print(f"[Harness] No central gRPC server found at {server_script}, skipping...")
                return

        if cmd:
            log_file = open(f'/tmp/{self.service}_server.log', 'w')
            self.server_proc = subprocess.Popen(cmd, stdout=log_file, stderr=subprocess.STDOUT)
            print(f"  [+] Server started, PID={self.server_proc.pid}")
            time.sleep(3) # Wait for startup

    def stop_server(self):
        print(f"[Harness] Stopping server/broker/router for {self.service}...")
        if self.service == 'activemq':
            service_path = self.get_service_path()
            cmd = [str(service_path / 'apache-activemq-6.2.0' / 'bin' / 'activemq'), 'stop']
            env = os.environ.copy()
            java17_home = '/usr/lib/jvm/java-17-openjdk-17.0.17.0.10-1.el8.x86_64'
            if os.path.isdir(java17_home):
                env['JAVA_HOME'] = java17_home
            try:
                subprocess.run(cmd, check=True, env=env)
            except subprocess.CalledProcessError:
                print("  [!] ActiveMQ stop command failed (maybe already stopped or failed to start)")
            return

        if self.server_proc:
            self.server_proc.terminate()
            try:
                self.server_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.server_proc.kill()
            print(f"  [-] Server stopped")

    def run(self):
        try:
            self.start_server()
            self.spawn_receivers()
            self.run_sender()
            results = self.aggregate_results()
            # Inject receiver metadata for generate_table.py
            results['py_receivers'] = self.py_receivers
            results['cpp_receivers'] = self.cpp_receivers
            return results
        finally:
            self.stop_receivers()
            self.stop_server()


def main():
    parser = argparse.ArgumentParser(description='Multi-Receiver Test Harness')
    parser.add_argument('--service', required=True, choices=['redis', 'rabbitmq', 'zeromq', 'nats', 'grpc', 'activemq'])
    parser.add_argument('--sender', required=True, choices=['python', 'cpp'])
    parser.add_argument('--py-receivers', type=int, default=16)
    parser.add_argument('--cpp-receivers', type=int, default=16)
    parser.add_argument('--report', help='File to append results to')
    
    args = parser.parse_args()
    
    if args.py_receivers + args.cpp_receivers != 32:
        print(f"Warning: Total receivers is {args.py_receivers + args.cpp_receivers}, expected 32")
    
    report_path = os.path.join(os.getcwd(), 'report.txt')
    if os.path.exists(report_path):
        os.remove(report_path) # Clear old results to only capture current run

    harness = TestHarness(
        service=args.service,
        sender_lang=args.sender,
        py_receivers=args.py_receivers,
        cpp_receivers=args.cpp_receivers
    )
    
    results = harness.run()

    # Try to read sender results from report.txt
    sender_results = {}
    if os.path.exists(report_path):
        try:
            with open(report_path, 'r') as f:
                lines = f.readlines()
                if lines:
                    # Take the last line in case multiple were written (shouldn't happen here)
                    sender_results = json.loads(lines[-1])
        except Exception as e:
            print(f"[Harness] Warning: Could not read sender results from {report_path}: {e}")

    # Merge results
    final_results = {**results, **sender_results}

    if args.report:
        with open(args.report, 'a') as f:
            f.write(json.dumps(final_results) + '\n')
        print(f"[Harness] Results appended to {args.report}")

    print("\n[Harness] Results:")
    print(json.dumps(final_results, indent=2))


if __name__ == '__main__':
    main()
