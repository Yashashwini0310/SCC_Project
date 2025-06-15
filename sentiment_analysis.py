from textblob import TextBlob
import os

base_path = "reduced_imdb"
output_file = "sentiment_output.txt"

with open(output_file, 'w', encoding='utf-8') as out:
    for sentiment in os.listdir(base_path):
        folder_path = os.path.join(base_path, sentiment)
        if os.path.isdir(folder_path):
            for filename in os.listdir(folder_path):
                file_path = os.path.join(folder_path, filename)
                if os.path.isfile(file_path):
                    with open(file_path, 'r', encoding='utf-8') as file:
                        content = file.read()
                        blob = TextBlob(content)
                        polarity = blob.sentiment.polarity
                        out.write(f"{filename} ({sentiment}): {polarity}\n")
print(f"[✓] Sentiment analysis completed. Results saved to {output_file}")
