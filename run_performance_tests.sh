#!/bin/bash
rm -f /home/ec2-user/streaming_data/*.txt
# Usage check
if [ "$1" != "sequential" ] && [ "$1" != "parallel" ]; then
    echo "Usage: bash run_performance_tests.sh [sequential|parallel]"
    exit 1
fi

# Ensure simulate_streaming.sh is executable
chmod +x simulate_streaming.sh

# Load sizes to test
LOAD_SIZES=(1 3 5 10)

for load in "${LOAD_SIZES[@]}"
do
    echo "======================================"
    echo "[Test] Starting benchmark for load size: $load"

    # Start streaming simulation in background
    ./simulate_streaming.sh $load &
    STREAM_PID=$!
    sleep 5 # Give it time to warm up

    # Run the appropriate benchmark script
    if [ "$1" == "sequential" ]; then
        echo "[Run] Running hybrid_sequential.py for load $load..."
        python3 hybrid_sequential.py $load
    else
        echo "[Run] Running hybrid_simulator.py for load $load..."
        python3 hybrid_simulator.py $load
    fi

    # Kill background stream simulation
    kill $STREAM_PID
    echo "[Cleanup] simulate_streaming.sh stopped (PID $STREAM_PID)"
    
    echo "[Test] Completed for workload: $load files"
    echo "======================================"
    echo ""
done

echo "[Done] All benchmark tests completed for mode: $1"
