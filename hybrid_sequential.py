import subprocess
import time
from benchmark_logger_csv import log_to_csv
import boto3
import botocore.exceptions

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
    subprocess.run(["python3", "sentiment_multiprocessing.py"])
    duration = time.time() - start
    log_to_csv("MapReduce", duration)
    print(f"[Sequential] MapReduce finished in {duration:.2f} sec")
    return duration

def run_spark():
    print("[Sequential] Running Spark Streaming...")
    start = time.time()
    subprocess.run(["spark-submit", "spark_streaming.py"])
    duration = time.time() - start
    log_to_csv("SparkStreaming", duration)
    print(f"[Sequential] Spark Streaming finished in {duration:.2f} sec")
    return duration

if __name__ == "__main__":
    print("[Sequential] Running in sequential mode...")
    start_time = time.time()

    map_time = run_mapreduce()
    spark_time = run_spark()

    total_time = time.time() - start_time
    log_to_csv("HybridSequential", total_time)
    print(f"[Sequential] Total sequential execution time: {total_time:.2f} sec")
    push_custom_metric("TotalSequentialExecutionTime", total_time)
