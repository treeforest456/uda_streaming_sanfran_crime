# Two questions
1. How did changing values on the SparkSession property parameters affect the throughput and latency of the data?
* By changing the parameters, we could check on the `processedRowsPerSecond`. If this number is going larger, then we have a higher throughput.

2. What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?
* spark.default.parallelism: we can set this number accoring to the number of machines and the number of cores of each machine to maximize the performance
* spark.streaming.kafka.maxRatePerPartition: this parameter can limit the max processing rate for each partition
