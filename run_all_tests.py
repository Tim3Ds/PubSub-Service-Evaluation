#!/usr/bin/env python3
import subprocess
import time
import sys
import argparse

class Logger:
    def __init__(self, filename):
        self.terminal = sys.stdout
        self.log = open(filename, "w")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
        self.log.flush()

    def flush(self):
        self.terminal.flush()
        self.log.flush()

def run_test(service, sender, py_receivers, cpp_receivers, report_file, logger, async_sender=False, async_receiver=False):
    mode_str = f"S:{'A' if async_sender else 'N'}/R:{'A' if async_receiver else 'N'}"
    print(f"[-] Running {service} {sender} ({mode_str}) -> {py_receivers} Py / {cpp_receivers} C++...")
    cmd = [
        "python3", "-u", "harness/test_harness.py",
        "--service", service,
        "--sender", sender,
        "--py-receivers", str(py_receivers),
        "--cpp-receivers", str(cpp_receivers),
        "--report", report_file
    ]
    if async_sender:
        cmd.append("--async-sender")
    if async_receiver:
        cmd.append("--async-receiver")
    try:
        # Capture stdout/stderr and print/log in real-time if possible, 
        # or just let it write to sys.stdout which is now our Logger
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        
        while True:
            output = process.stdout.readline()
            if output == '' and process.poll() is not None:
                break
            if output:
                print(output, end='') # This goes to Logger.write
                
        rc = process.poll()
        if rc == 0:
            print("[+] Test Completed\n")
        else:
            print(f"[!] Test failed with exit code {rc}\n")
            
    except Exception as e:
        print(f"[!] Test failed with exception: {e}\n")

def main():
    parser = argparse.ArgumentParser(description="Run messaging service tests")
    parser.add_argument("--service", type=str, help="Run tests for a specific service (e.g., zeromq, grpc, redis, rabbitmq, nats, activemq)")
    parser.add_argument("--all", action="store_true", help="Run all services (default behavior)")
    args = parser.parse_args()
    
    all_services = ['grpc', 'zeromq', 'redis', 'rabbitmq', 'nats', 'activemq']
    
    # Determine which services to run
    if args.service:
        if args.service not in all_services:
            print(f"Error: Unknown service '{args.service}'")
            print(f"Available services: {', '.join(all_services)}")
            sys.exit(1)
        services = [args.service]
    else:
        services = all_services
    
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    report_file = f"report{timestamp}.json"
    log_file = f"run_log_{timestamp}.txt"
    
    # Redirect stdout to Logger
    sys.stdout = Logger(log_file)
    
    print(f"Results will be written to {report_file}")
    print(f"Log will be written to {log_file}")
    print(f"Services to test: {', '.join(services)}")

    spreads = [
        # (32, 0),
        # (31, 1),
        # (24, 8),
        # (20, 12),
        (16, 16),
        # (8, 24),
        # (1, 31),
        # (0, 32)
    ]

    scenarios = []
    
    for service in services:
        for sender in ['python', 'cpp']:
            for py, cpp in spreads:
                for async_s in [False, True]:
                    for async_r in [False, True]:
                        scenarios.append((service, sender, py, cpp, async_s, async_r))

    count = len(scenarios)
    print(f"Starting execution of {count} test scenarios...")
    start_time = time.time()

    for i, (service, sender, py, cpp, async_s, async_r) in enumerate(scenarios):
        print(f"Scenario {i+1}/{count}")
        run_test(service, sender, py, cpp, report_file, sys.stdout, async_s, async_r)
        # Small cooldown to ensure ports allow release if needed
        time.sleep(1) 

    duration = time.time() - start_time
    print(f"All tests completed in {duration:.2f} seconds.")

if __name__ == "__main__":
    main()

