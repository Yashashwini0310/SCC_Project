import os
from multiprocessing import Pool, cpu_count
from textblob import TextBlob
import boto3

# === CONFIG ===
input_dirs = ["reduced_imdb/pos", "reduced_imdb/neg"]
output_file = "sentiment_mapreduce_output.txt"
bucket_name = "imdb-text-processing"  
s3_key = "results/" + output_file

# === TASK ===
def analyze_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            text = f.read()
        sentiment = TextBlob(text).sentiment.polarity
        return (file_path, "positive" if sentiment > 0 else "negative")
    except Exception as e:
        return (file_path, "error")

# === COLLECT ALL FILES ===
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
    files = collect_all_files()
    print(f"Processing {len(files)} files using {cpu_count()} cores...")

    with Pool(cpu_count()) as p:
        results = p.map(analyze_file, files)

    # Aggregate Results
    positive = sum(1 for _, label in results if label == "positive")
    negative = sum(1 for _, label in results if label == "negative")
    error = sum(1 for _, label in results if label == "error")

    with open(output_file, "w") as f:
        f.write(f"Total Files: {len(results)}\n")
        f.write(f"Positive Reviews: {positive}\n")
        f.write(f"Negative Reviews: {negative}\n")
        f.write(f"Errors: {error}\n")

    print("Analysis complete. Uploading to S3...")

    # Upload to S3
    s3 = boto3.client('s3')
    s3.upload_file(output_file, bucket_name, s3_key)
    print(f"Uploaded to S3://{bucket_name}/{s3_key}")
