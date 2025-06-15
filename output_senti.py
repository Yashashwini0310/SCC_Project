import boto3

s3 = boto3.client('s3')

bucket_name = 'imdb-text-processing'  # replace with your actual bucket name
local_file = 'sentiment_output.txt'
s3_key = 'results/sentiment_output.txt'

s3.upload_file(local_file, bucket_name, s3_key)
print(f"[✓] File '{local_file}' uploaded to 's3://{bucket_name}/{s3_key}'")
