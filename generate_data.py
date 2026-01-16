import json
import random

messages = 10

def generate_random_hex_16bit():
    return f"{random.randint(0, 0xFFFF):04X}"

data = []
for i in range(messages):
    data.append({
        "message_id": i + 1,
        "message_name": f"test_{i+1}",
        "message_value": [generate_random_hex_16bit() for _ in range(random.randint(3, 32))],
        "target": i % 32  # Distribute messages across all 32 receivers
    })

with open('test_data.json', 'w') as f:
    json.dump(data, f, indent=4)

print(f"Generated {messages} messages in test_data.json (distributed across 32 receivers)")
