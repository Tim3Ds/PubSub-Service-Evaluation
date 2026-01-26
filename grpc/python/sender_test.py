"""
gRPC Sender with targeted routing support for multi-receiver testing.
Routes each message to localhost:5005{target} based on the target field.
"""
import grpc
import test_data_pb2
import test_data_pb2_grpc
import json
import sys
import os
import time

# Add harness to path for stats_collector
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../harness'))
from stats_collector import MessageStats, get_current_time_ms

def main():
    data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../test_data.json'))
    with open(data_path, 'r') as f:
        test_data = json.load(f)

    # Create channels for all 32 receivers (ports 50051-50082)
    channels = {}
    stubs = {}
    for i in range(32):
        port = 50051 + i
        channels[i] = grpc.insecure_channel(f'localhost:{port}')
        stubs[i] = test_data_pb2_grpc.TestDataServiceStub(channels[i])

    stats = MessageStats()
    stats.start_time = get_current_time_ms()

    print(f" [x] Starting transfer of {len(test_data)} messages...")

    for item in test_data:
        target = item.get('target', 0)
        msg_start = get_current_time_ms()
        print(f" [x] Sending message {item['message_id']} to target {target}...", end='', flush=True)
        try:
            request = test_data_pb2.TestDataItem(
                message_id=item['message_id'],
                message_name=item['message_name'],
                message_value=item['message_value'],
                target=target
            )
            response = stubs[target].TransferData(request, timeout=0.04)
            
            if response.status == 'ACK' and response.message_id == item['message_id']:
                msg_duration = get_current_time_ms() - msg_start
                stats.record_message(True, msg_duration)
                print(" [OK]")
            else:
                stats.record_message(False)
                print(f" [FAILED] Unexpected response: {response.status}")
        except Exception as e:
            stats.record_message(False)
            print(f" [FAILED] Error: {e}")

    stats.end_time = get_current_time_ms()
    
    report = {
        "service": "gRPC",
        "language": "Python",
        **stats.get_stats()
    }

    print("\nTest Results:")
    for k, v in report.items():
        if k != 'message_timing_stats':
            print(f"{k}: {v}")

    report_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../report.txt'))
    with open(report_path, 'a') as f:
        f.write(json.dumps(report) + "\n")

    # Close channels
    for ch in channels.values():
        ch.close()

if __name__ == "__main__":
    main()
