# Section 4 - Notes

## Lesson 4.3 Exercises

Thought experiments with a cluster:

- a dataset X GB in size is being shuffled for a complex job
- shuffle is required, but job taking too long
- optimize the shuffle: pick the correct number of partitions to use

Things to keep in mind:

- optimal partition size 10-100MB
- largest desired partition size 200MB
- CPU cores must not be idle

### Scenario 1

- you see a shuffle write of 210GB in the Spark UI
- job takes a long time
- you have 4 executors with 4 cores each

Solution:

- For executors at least 4 * 4 = 16 partitions
- Largest partition size of 200MB, at least 1050 partition
- Pick the maximum of the two: spark.sql.shuffle.partitions = 1050

Result:

- optimal time/task as partition is of optimal size
- 100% cluster utilization

Recommendation:

- allocate more CPU cores

### Scenario 2

- you see a shuffle write of 210GB in the Spark UI
- job takes a long time
- you have 200 executors with 8 cores each

Solution:

- For executors at least 200 * 8 = 1600 partitions
- Largest partition size of 200MB, at least 1050 partition
- Pick the maximum of the two: spark.sql.shuffle.partitions = 1600

Result:

- Partition size of ~134 MB (OPTIMAL)
- 100% cluster utilization and high parallelism

Recommendation:

- Add more memory to worker nodes

### Scenario 3

- you see a shuffle write of ~1GB in the Spark UI
- job takes a long time
- you have 2 executors with 4 cores each

Solution:

- For executors at least 2 * 4 = 8 partitions
- Largest partition size of 200MB, at least 5 partition
- Smallest partition size of 10MB, at least 100 partition
- Pick the maximum of the two: spark.sql.shuffle.partitions = 10

Result:

- Partition size of 100 MB (OPTIMAL)
- 100% cluster utilization
- 100x overhead reduction from task creation compared to 10000 partitions => 3-10x perf boost

Recommendation:

- Cluster is pretty adequate for the job
