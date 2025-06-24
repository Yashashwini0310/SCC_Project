#!/bin/bash

# Make sure scripts are executable
chmod +x simulate_streaming.sh

# Define workloads to test (number of files per batch)
for load in 1 3 5 10
do
    echo "--------------------------------------"
    echo "[Test] Running for workload: $load files"
    
    # Simulate streaming with current load
    ./simulate_streaming.sh $load

    # Run the processing logic (choose one)
    python3 hybrid_parallel.py  # or python3 hybrid_sequential.py

    echo "[Test] Completed for workload: $load files"
    echo "--------------------------------------"
    echo ""
done
