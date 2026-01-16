import json
import os
import glob
import argparse
from collections import defaultdict

def main():
    parser = argparse.ArgumentParser(description='Generate performance table from report.')
    parser.add_argument('--report', help='Path to report file')
    args = parser.parse_args()

    if args.report:
        report_path = args.report
    else:
        # Find the latest report file
        report_files = glob.glob('report*.json')
        if not report_files:
            # Fallback to report.txt if no JSON reports found
            report_path = 'report.txt'
        else:
            # Sort by modification time to get the latest
            report_path = max(report_files, key=os.path.getmtime)
    
    if not os.path.exists(report_path):
        print(f"No report file found (tried {report_path})")
        return

    print(f"Reading data from {report_path}")
    data = []
    with open(report_path, 'r') as f:
        for line in f:
            try:
                if line.strip():
                    data.append(json.loads(line))
            except json.JSONDecodeError:
                continue

    # Group runs by service and sender
    # test_runs[service][sender_lang] = [run1, run2, ...]
    test_runs = defaultdict(lambda: defaultdict(list))
    
    for entry in data:
        svc = entry.get('service')
        # Normalize service names to match order if needed, but we'll use keys from data
        lang = entry.get('language') or entry.get('sender_lang')
        if svc and lang:
            # Normalize casing
            svc_norm = svc.lower()
            lang_norm = lang.capitalize()
            test_runs[svc_norm][lang_norm].append(entry)

    # Calculate peak performance for each service to determine sort order
    service_peaks = {}
    for svc, senders in test_runs.items():
        peak = 0
        for lang, runs in senders.items():
            for run in runs:
                peak = max(peak, run.get('processed_per_ms', 0))
        service_peaks[svc] = peak

    # Sort services by peak performance (descending)
    sorted_services = sorted(service_peaks.keys(), key=lambda x: service_peaks[x], reverse=True)

    spreads = [
        "32 Py / 0 C++", "31 Py / 1 C++", "24 Py / 8 C++", "20 Py / 12 C++", "16 Py / 16 C++", "8 Py / 24 C++", "0 Py / 32 C++"
    ]
    
    print("| Service | Sender | Receiver Split | Throughput (msgs/ms) | Success Rate |")
    print("|---|---|---|---|---|")
    
    for svc_norm in sorted_services:
         # Find original display name from data
         display_name = next(run.get('service') for senders in test_runs[svc_norm].values() for run in senders)
         
         for sender in ['Python', 'C++']:
             if sender not in test_runs[svc_norm]:
                 continue
                 
             runs = test_runs[svc_norm][sender]
             
             # Instead of assuming the last 7, let's use the actual py/cpp receiver counts from the data if available
             # or fallback to the enumeration if they are in order.
             for run in runs:
                 py = run.get('py_receivers')
                 cpp = run.get('cpp_receivers')
                 if py is not None and cpp is not None:
                     split_name = f"{py} Py / {cpp} C++"
                 else:
                     # Fallback to enumeration logic if metadata missing
                     # This is just for backward compatibility with old report.txt
                     idx = runs.index(run)
                     split_name = spreads[idx] if idx < len(spreads) else "Unknown"
                     
                 tput = run.get('processed_per_ms', 0)
                 total = run.get('total_sent', 400)
                 processed = run.get('total_processed', 0)
                 success_rate = (processed / total) * 100 if total > 0 else 0
                 
                 tput_str = f"{tput:.2f}"
                 if tput > 2.0:
                     tput_str = f"**{tput_str}**"
                     
                 print(f"| {display_name} | {sender} | {split_name} | {tput_str} | {success_rate:.1f}% |")

if __name__ == '__main__':
    main()
