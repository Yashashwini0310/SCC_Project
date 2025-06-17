import csv
import os

def log_to_csv(method, duration):
    file_exists = os.path.isfile("benchmark_results.csv")
    with open("benchmark_results.csv", "a", newline="") as csvfile:
        writer = csv.writer(csvfile)
        if not file_exists:
            writer.writerow(["Method", "ExecutionTimeSec"])
        writer.writerow([method, round(duration, 2)])
