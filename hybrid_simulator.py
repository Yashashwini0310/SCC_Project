import subprocess
import threading
import time
import boto3
import botocore.exceptions
from benchmark_logger_csv import log_to_csv
import psutil
import sys

load_sizes = [1, 3, 5]  # Simulate 1, 3, and 5 file batches (adjustable)


# CloudWatch Metric Sender
def push_custom_metric(metric_name, value):
    cloudwatch = boto3.client('cloudwatch')
    try:
        cloudwatch.put_metric_data(
            Namespace='IMDbHybridBenchmark',
            MetricData=[
                {
                    'MetricName': metric_name,
                    'Dimensions': [
                        {'Name': 'Application', 'Value': 'IMDbHybridRunner'},
                    ],
                    'Value': value,
                    'Unit': 'Seconds'
                },
            ]
        )
        print(f"[CloudWatch] Pushed {metric_name}: {value:.2f} sec")
    except botocore.exceptions.ClientError as e:
        print("[CloudWatch Error]", e)

def run_mapreduce():
    print("[Hybrid] Starting MapReduce...")
    start = time.time()
    subprocess.run(["python3", "sentiment_multiprocessing.py"], check=True)
    duration = time.time() - start
    cpu = psutil.cpu_percent(interval=1)
    log_to_csv("MapReduce", duration,cpu)
    print(f"[Hybrid] MapReduce finished in {time.time() - start:.2f} sec")
    return duration

def run_spark():
    print("[Hybrid] Starting Spark Streaming...")
    start = time.time()
    try:
        result = subprocess.run(["spark-submit", "spark_streaming.py"], capture_output=True, text=True, timeout=180)
        print("[Spark STDOUT]", result.stdout)
        print("[Spark STDERR]", result.stderr)
    except subprocess.TimeoutExpired:
        print("[Error] No files were publishes via spark_streaming.py and was terminated.")
    duration = time.time() - start
    cpu = psutil.cpu_percent(interval=1)
    log_to_csv("SparkStreaming", duration, cpu)
    print(f"[Hybrid] Spark Streaming finished in {time.time() - start:.2f} sec")
    return duration

if __name__ == "__main__":
    for load in load_sizes:
        print(f"\n[Hybrid] Running test with load size: {load}")

        # Launch batch and streaming in parallel
        start_time = time.time()

        def run_mr():
            duration = run_mapreduce()
            # log_to_csv("MapReduce", duration, psutil.cpu_percent(), load)

        def run_sp():
            duration = run_spark()
            # log_to_csv("SparkStreaming", duration, psutil.cpu_percent(), load)

        t1 = threading.Thread(target=run_mr)
        t2 = threading.Thread(target=run_sp)

        t1.start()
        t2.start()

        t1.join()
        t2.join()

        total_duration = time.time() - start_time
        total_cpu = psutil.cpu_percent(interval=1)
        log_to_csv("HybridParallel", total_duration, total_cpu, load)

        push_custom_metric("TotalHybridExecutionTime", total_duration)
        print(f"[Hybrid] Finished test for load size {load} in {total_duration:.2f} sec")
