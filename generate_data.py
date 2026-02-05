import json
import random
import argparse

def generate_random_hex_16bit():
    return f"{random.randint(0, 0xFFFF):04X}"

def main():
    parser = argparse.ArgumentParser(description="Generate test data for messaging service evaluation")
    parser.add_argument("--messages", type=int, default=35, help="Number of messages to generate (default: 35)")
    parser.add_argument("--receivers", type=int, default=32, help="Number of receivers to distribute messages across (default: 32)")
    args = parser.parse_args()

    messages = args.messages
    receivers = args.receivers

    data = []
    for i in range(messages):
        data.append({
            "message_id": i + 1,
            "message_name": f"test_{i+1}",
            "message_value": [generate_random_hex_16bit() for _ in range(random.randint(3, 32))],
            "target": i % receivers  # Distribute messages across all specified receivers
        })

    with open('test_data.json', 'w') as f:
        json.dump(data, f, indent=4)

    print(f"Generated {messages} messages in test_data.json (distributed across {receivers} receivers)")

if __name__ == "__main__":
    main()
