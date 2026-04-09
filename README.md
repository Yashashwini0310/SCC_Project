Conducted large-scale NLP analysis on the IMDb movie dataset, benchmarking hybrid parallel processing against sequential methods across three workload sizes (5 MB, 200 MB, ~2 GB).

Built a hybrid processing model combining Python Multiprocessing with MapReduce and Apache Spark Streaming for real-time batch simulation. Performed sentiment analysis and keyword extraction on all dataset loads, logging CPU utilisation, execution time, and performance metrics to CSV files with Matplotlib visualisations.

Results demonstrated significant efficiency gains with the hybrid approach at scale, particularly for 2 GB loads — providing clear evidence for cloud-based NLP workload design decisions. Deployed and tested on AWS Cloud9 with metrics stored and monitored via S3 and CloudWatch.

Key skills demonstrated: distributed computing, big data processing, cloud performance benchmarking, NLP pipelines.
