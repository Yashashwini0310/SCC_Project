import os
import csv
from datetime import datetime

def log_to_csv(method, exec_time, cpu=None, timestamp=None):
    filename = "benchmark_results.csv"
    file_exists = os.path.isfile(filename)

    if not timestamp:
        timestamp = datetime.now().isoformat()

    with open(filename, "a", newline="") as f:
        writer = csv.writer(f)

        # Write header only if file is new
        if not file_exists:
            writer.writerow(["Method", "ExecutionTimeSec", "CPUUsage (%)", "Timestamp"])

        row = [method, round(exec_time, 2)]
        row.append(round(cpu, 2) if cpu is not None else "N/A")
        row.append(timestamp)
        writer.writerow(row)
