from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import os

# Create SparkContext and StreamingContext with batch interval of 10 seconds
sc = SparkContext(appName="IMDbReviewStreaming")
ssc = StreamingContext(sc, 10)

# Monitor directory for new text files
input_dir = "/home/ubuntu/streaming_data"
lines = ssc.textFileStream(input_dir)

# Split lines into words
words = lines.flatMap(lambda line: line.split(" "))

# Filter stopwords (optional)
stopwords = ['the', 'and', 'was', 'is', 'a', 'an', 'of', 'to', 'in', 'on']
filtered = words.filter(lambda word: word.lower() not in stopwords)

# Word count
word_counts = filtered.map(lambda word: (word.lower(), 1)).reduceByKey(lambda a, b: a + b)

# Print the word counts
word_counts.pprint()

# Start streaming
ssc.start()
ssc.awaitTermination()

