import subprocess
import threading
import time
import boto3
import botocore.exceptions
from benchmark_logger_csv import log_to_csv

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
    log_to_csv("MapReduce", duration)
    print(f"[Hybrid] MapReduce finished in {time.time() - start:.2f} sec")
    return duration

def run_spark():
    print("[Hybrid] Starting Spark Streaming...")
    start = time.time()
    subprocess.run(["spark-submit", "spark_streaming.py"], check=True)
    duration = time.time() - start
    log_to_csv("SparkStreaming", duration)
    print(f"[Hybrid] Spark Streaming finished in {time.time() - start:.2f} sec")
    return duration

if __name__ == "__main__":
    start_time = time.time()
    print("[Hybrid] Launching batch and stream jobs...")
    
    mapreduce_time = [0]
    spark_time = [0]
    
    def run_mr(): mapreduce_time[0] = run_mapreduce()
    def run_sp(): spark_time[0] = run_spark()

    t1 = threading.Thread(target=run_mr)
    t2 = threading.Thread(target=run_sp)

    t1.start()
    t2.start()

    t1.join()
    t2.join()

    total_duration = time.time() - start_time
    log_to_csv("HybridParallel", total_duration)
    print(f"[Hybrid] Total hybrid execution time: {total_duration:.2f} sec")
    push_custom_metric("TotalHybridExecutionTime", total_duration)