# #!/bin/bash

# # Paths
# POS_DIR="reduced_imdb/pos"
# NEG_DIR="reduced_imdb/neg"
# TARGET_DIR="/home/ec2-user/streaming_data"

# # Create target if not exists
# mkdir -p "$TARGET_DIR"

# echo "[Streaming Feed] Starting to simulate real-time review drops..."

# for i in {1..5}; do
#     # Copy random pos and neg review each
#     POS_FILE=$(ls "$POS_DIR" | shuf -n 1)
#     NEG_FILE=$(ls "$NEG_DIR" | shuf -n 1)

#     cp "$POS_DIR/$POS_FILE" "$TARGET_DIR/pos_review_$i.txt"
#     cp "$NEG_DIR/$NEG_FILE" "$TARGET_DIR/neg_review_$i.txt"

#     echo "[Streaming Feed] Dropped batch $i to $TARGET_DIR"
#     sleep 10
# done

# echo "[Streaming Feed] Finished streaming simulation."
#!/bin/bash

# Accept batch count as parameter
NUM_BATCHES=$1
if [ -z "$NUM_BATCHES" ]; then
    NUM_BATCHES=5  # Default to 5 if no argument
fi

POS_DIR="reduced_imdb/pos"
NEG_DIR="reduced_imdb/neg"
TARGET_DIR="/home/ec2-user/streaming_data"

mkdir -p "$TARGET_DIR"

echo "[Streaming Feed] Simulating $NUM_BATCHES batches..."

for ((i=1; i<=NUM_BATCHES; i++)); do
    POS_FILE=$(ls "$POS_DIR" | shuf -n 1)
    NEG_FILE=$(ls "$NEG_DIR" | shuf -n 1)
    TIMESTAMP=$(date +%s%N)  # Nanosecond timestamp

    cp "$POS_DIR/$POS_FILE" "$TARGET_DIR/pos_review_${i}_${TIMESTAMP}.txt"
    cp "$NEG_DIR/$NEG_FILE" "$TARGET_DIR/neg_review_${i}_${TIMESTAMP}.txt"

    echo "[Streaming Feed] Dropped batch $i to $TARGET_DIR"
    sleep 1
done

echo "[Streaming Feed] Finished streaming simulation."
