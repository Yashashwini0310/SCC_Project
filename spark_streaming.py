from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from textblob import TextBlob
import os
import time
import boto3

# Setup Spark Context and Streaming Context (5s batches)
sc = SparkContext(appName="IMDbReviewStreaming")
ssc = StreamingContext(sc, 5)

# Monitor the input directory
input_dir = "/home/ec2-user/streaming_data"
lines = ssc.textFileStream(input_dir)

# Define stopwords
stopwords = ['the', 'and', 'was', 'is', 'a', 'an', 'of', 'to', 'in', 'on']

# Sentiment Analysis Function
def analyze_sentiment(line):
    blob = TextBlob(line)
    polarity = blob.sentiment.polarity
    if polarity > 0:
        return ("positive", 1)
    elif polarity < 0:
        return ("negative", 1)
    else:
        return ("neutral", 1)

# 1️⃣ Standard word count (non-windowed)
words = lines.flatMap(lambda line: line.split(" "))
filtered_words = words.filter(lambda word: word.lower() not in stopwords)
word_pairs = filtered_words.map(lambda word: (word.lower(), 1))
word_counts = word_pairs.reduceByKey(lambda a, b: a + b)

# 2️⃣ Sentiment counts (non-windowed)
sentiments = lines.map(analyze_sentiment)
sentiment_counts = sentiments.reduceByKey(lambda a, b: a + b)

# 3️⃣ Sliding window word counts
windowed_words = words.window(30, 10)
windowed_filtered = windowed_words.filter(lambda word: word.lower() not in stopwords)
windowed_word_pairs = windowed_filtered.map(lambda word: (word.lower(), 1))
windowed_word_counts = windowed_word_pairs.reduceByKey(lambda a, b: a + b)

# Print to terminal (debug)
word_counts.pprint()
sentiment_counts.pprint()
windowed_word_counts.pprint()

# Save results to local directory
timestamp = int(time.time())
word_counts.saveAsTextFiles(f"stream_output/wordcount_{timestamp}")
sentiment_counts.saveAsTextFiles(f"stream_output/sentiment_{timestamp}")
windowed_word_counts.saveAsTextFiles(f"stream_output/windowed_wordcount_{timestamp}")

# Upload to S3
def upload_to_s3(local_folder, bucket_name, s3_folder):
    s3 = boto3.client('s3')
    for root, dirs, files in os.walk(local_folder):
        for file in files:
            local_path = os.path.join(root, file)
            s3_key = f"{s3_folder}/{file}"
            s3.upload_file(local_path, bucket_name, s3_key)
            print(f"Uploaded: {s3_key}")

# Start Streaming
ssc.start()
print("Spark Streaming Started. Waiting for input...")
ssc.awaitTerminationOrTimeout(60)
print("Streaming stopped. Uploading to S3...")
upload_to_s3("stream_output", "imdb-text-processing", "results")
