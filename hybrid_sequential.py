import subprocess
import time
from benchmark_logger_csv import log_to_csv
import boto3
import botocore.exceptions
import psutil
import sys, glob, os

load_sizes = [1, 3, 5, 10]  # You can increase or adjust these if needed
def get_load_size_mb(directory, limit):
    files = sorted(glob.glob(os.path.join(directory, "*.txt")))[:limit]
    return round(sum(os.path.getsize(f) for f in files) / (1024 * 1024), 2)


def push_custom_metric(metric_name, value):
    cloudwatch = boto3.client('cloudwatch')
    try:
        cloudwatch.put_metric_data(
            Namespace='IMDbHybridBenchmark',
            MetricData=[
                {
                    'MetricName': metric_name,
                    'Dimensions': [{'Name': 'Application', 'Value': 'IMDbHybridSequential'}],
                    'Value': value,
                    'Unit': 'Seconds'
                },
            ]
        )
        print(f"[CloudWatch] Pushed {metric_name}: {value:.2f} sec")
    except botocore.exceptions.ClientError as e:
        print("[CloudWatch Error]", e)

def run_mapreduce():
    print("[Sequential] Running MapReduce...")
    start = time.time()
    subprocess.run(["python3", "sentiment_multiprocessing.py"],timeout=30)
    duration = time.time() - start
    cpu = psutil.cpu_percent(interval=1)
    # log_to_csv("MapReduce", duration, cpu)
    print(f"[Sequential] MapReduce finished in {duration:.2f} sec")
    return duration

def run_spark():
    print("[Sequential] Running Spark Streaming...")
    start = time.time()
    try:
        result = subprocess.run(["spark-submit", "spark_streaming.py"], capture_output=True, text=True, timeout=30)
        print("[Spark STDOUT]", result.stdout)
        print("[Spark STDERR]", result.stderr)
    except subprocess.TimeoutExpired:
        print("[Error] No files were publishes via spark_streaming.py and was terminated.")
    duration = time.time() - start
    cpu = psutil.cpu_percent(interval=1)
    # log_to_csv("SparkStreaming", duration, cpu)
    print(f"[Sequential] Spark Streaming finished in {duration:.2f} sec")
    return duration

if __name__ == "__main__":
    for load in load_sizes:
        print(f"\n[Sequential] Running test with load size: {load}")
        
        load_size_mb = get_load_size_mb("streaming_data", load)
        load_label = f"{load} files - {load_size_mb} MB"
        
        start_time = time.time()

        map_time = run_mapreduce()
        log_to_csv("MapReduce", map_time, psutil.cpu_percent(), load, load_label)

        spark_time = run_spark()
        log_to_csv("SparkStreaming", spark_time, psutil.cpu_percent(), load, load_label)

        total_time = time.time() - start_time
        total_cpu = psutil.cpu_percent(interval=1)
        log_to_csv("HybridSequential", total_time, total_cpu, load, load_label)

        push_custom_metric("TotalSequentialExecutionTime", total_time)
        print(f"[Sequential] Finished test for load size {load} in {total_time:.2f} sec")
