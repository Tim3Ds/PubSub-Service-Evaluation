#!/usr/bin/env python3
import subprocess
import time
import sys

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

def run_test(service, sender, py_receivers, cpp_receivers, report_file, logger):
    print(f"[-] Running {service} {sender} -> {py_receivers} Py / {cpp_receivers} C++...")
    cmd = [
        "python3", "harness/test_harness.py",
        "--service", service,
        "--sender", sender,
        "--py-receivers", str(py_receivers),
        "--cpp-receivers", str(cpp_receivers),
        "--report", report_file
    ]
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
            print("[+] Test passed\n")
        else:
            print(f"[!] Test failed with exit code {rc}\n")
            
    except Exception as e:
        print(f"[!] Test failed with exception: {e}\n")

def main():
    services = ['grpc', 'zeromq', 'redis', 'rabbitmq', 'nats']
    # ActiveMQ excluded due to known performance issues/timeouts
    
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    report_file = f"report{timestamp}.json"
    log_file = f"run_log_{timestamp}.txt"
    
    # Redirect stdout to Logger
    sys.stdout = Logger(log_file)
    
    print(f"Results will be written to {report_file}")
    print(f"Log will be written to {log_file}")

    spreads = [
        (32, 0),
        (31, 1),
        (24, 8),
        (20, 12),
        (16, 16),
        (8, 24),
        (1, 31),
        (0, 32)
    ]

    scenarios = []
    
    for service in services:
        for sender in ['python', 'cpp']:
            for py, cpp in spreads:
                scenarios.append((service, sender, py, cpp))

    print(f"Starting execution of {len(scenarios)} test scenarios...")
    start_time = time.time()

    for service, sender, py, cpp in scenarios:
        run_test(service, sender, py, cpp, report_file, sys.stdout)
        # Small cooldown to ensure ports allow release if needed
        time.sleep(1) 

    duration = time.time() - start_time
    print(f"All tests completed in {duration:.2f} seconds.")

if __name__ == "__main__":
    main()
