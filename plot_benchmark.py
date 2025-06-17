import pandas as pd
import matplotlib.pyplot as plt
import csv


# Read the benchmark CSV file
df = pd.read_csv("benchmark_results.csv")

# Group by method and calculate average execution time
avg_times = df.groupby("Method")["ExecutionTimeSec"].mean().reset_index()
for i, val in enumerate(avg_times["ExecutionTimeSec"]):
    plt.text(i, val + 0.1, f"{val:.2f}s", ha='center', va='bottom')

# Plotting the bar chart
plt.figure(figsize=(8, 5))
plt.bar(avg_times["Method"], avg_times["ExecutionTimeSec"], color=["skyblue", "orange", "green"])

plt.title("Benchmark Comparison: MapReduce vs Spark Streaming vs Hybrid Parallelism")
plt.xlabel("Method")
plt.ylabel("Average Execution Time (sec)")
plt.grid(axis="y", linestyle="--", alpha=0.7)
plt.xticks(rotation=10)
# Save the plot
plt.tight_layout()
plt.savefig("benchmark_comparison.png")
plt.show()
