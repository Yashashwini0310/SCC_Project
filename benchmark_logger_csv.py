import os
import csv
from datetime import datetime

def log_to_csv(method, exec_time, cpu=None, load_size=None, timestamp=None):
    filename = "benchmark_results.csv"
    file_exists = os.path.isfile(filename)

    if not timestamp:
        timestamp = datetime.now().isoformat()

    with open(filename, "a", newline="") as f:
        writer = csv.writer(f)

        # Write header only if file is new
        if not file_exists:
            writer.writerow(["Method", "ExecutionTimeSec", "CPUUsage (%)", "Load", "Timestamp"])

        row = [
            method,
            round(exec_time, 2),
            round(cpu, 2) if cpu is not None else "N/A",
            load_size if load_size is not None else "N/A",
            timestamp
        ]

        writer.writerow(row)
