import os
import time
from multiprocessing import Pool, cpu_count
from textblob import TextBlob
import boto3

try:
    import psutil # for CPU usage
    use_psutil = True
except ImportError:
    use_psutil = False

# === CONFIG ===
input_dirs = ["reduced_imdb/pos", "reduced_imdb/neg"]
output_file = "sentiment_mapreduce_output.txt"
bucket_name = "imdb-text-processing"
s3_key = "results/" + output_file

# === MAP PHASE ===
def analyze_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            text = f.read()
        sentiment = TextBlob(text).sentiment.polarity
        return (file_path, "positive" if sentiment > 0 else "negative")
    except Exception:
        return (file_path, "error")

# === FILE GATHERING ===
def collect_all_files():
    files = []
    for dir_path in input_dirs:
        for file in os.listdir(dir_path):
            full_path = os.path.join(dir_path, file)
            if os.path.isfile(full_path):
                files.append(full_path)
    return files

# === MAIN ===
if __name__ == "__main__":
    start_time = time.time()

    files = collect_all_files()
    print(f"[INFO] Files to process: {len(files)}")
    print(f"[INFO] Using {cpu_count()} CPU cores")

    if use_psutil:
        print(f"[INFO] Initial CPU usage: {psutil.cpu_percent()}%")

    print("[MAP] Starting sentiment analysis...")
    with Pool(cpu_count()) as p:
        results = p.map(analyze_file, files)
    print("[MAP] Completed.")

    # === REDUCE PHASE ===
    print("[REDUCE] Aggregating sentiment counts...")
    positive = sum(1 for _, label in results if label == "positive")
    negative = sum(1 for _, label in results if label == "negative")
    error = sum(1 for _, label in results if label == "error")
    print("[REDUCE] Completed.")

    # === SAVE PHASE ===
    print(f"[SAVE] Writing results to {output_file}...")
    with open(output_file, "w") as f:
        f.write(f"Total Files: {len(results)}\n")
        f.write(f"Positive Reviews: {positive}\n")
        f.write(f"Negative Reviews: {negative}\n")
        f.write(f"Errors: {error}\n")
    print("[SAVE] Done.")

    # === S3 UPLOAD PHASE ===
    print("[UPLOAD] Uploading to S3...")
    s3 = boto3.client('s3')
    s3.upload_file(output_file, bucket_name, s3_key)
    print(f"[UPLOAD] File uploaded to s3://{bucket_name}/{s3_key}")

    # === EXECUTION TIME ===
    end_time = time.time()
    duration = end_time - start_time
    print(f"[DONE] Execution Time: {duration:.2f} seconds")

    if use_psutil:
        print(f"[INFO] Final CPU usage: {psutil.cpu_percent()}%")
