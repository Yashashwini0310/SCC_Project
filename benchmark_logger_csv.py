import os
import csv, glob
from datetime import datetime

def get_load_size_mb(directory, limit):
    files = sorted(glob.glob(os.path.join(directory, "*.txt")))[:limit]
    return round(sum(os.path.getsize(f) for f in files) / (1024 * 1024), 2)

def log_to_csv(method, exec_time, cpu=None, load_size=None, load_label=None, timestamp=None):
    filename = "benchmark_results.csv"
    file_exists = os.path.isfile(filename)

    if not timestamp:
        timestamp = datetime.now().isoformat()

    with open(filename, "a", newline="") as f:
        writer = csv.writer(f)

        # Write header only if file is new
        if not file_exists:
            writer.writerow(["Method", "ExecutionTimeSec", "CPUUsage (%)", "Load","load_label", "Timestamp"])

        row = [
            method,
            round(exec_time, 2),
            round(cpu, 2) if cpu is not None else "N/A",
            load_size if load_size is not None else "N/A",
            load_label,
            timestamp
        ]

        writer.writerow(row)
