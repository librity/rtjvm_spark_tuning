# Section 5 - Notes

## Lesson 1 Exercises

### Scenario 1

A HDFS & YARN cluster with

- 10 machines
- 64GB RAM and 16CPUs per machine

Solution:

- keep 4 cores and 4GB RAM per machine => 40 CPUs and 40 GB RAM for YARN, OS, daemons
- remaining: 120 CPUs, 600 GB RAM
- AM is negligible, will take down from executor mem
- 5 cores / executor => 24 executors
- memory/executor = 600/24 = 25 GB
- minus ~8% overhead => net executor memory 22-23 GB

```bash
$ spark-submit --num-executors 24 --executor-memory 22g --executor-cores 5
```

### Scenario 2

A large HDFS & YARN cluster on AWS with

- 1 master node r5.12xlarge
- 19 r5.12xlarge worker nodes
- 8 TB total RAM
- 960 total virtual CPUs

Solution:

- leave the master node alone – will keep AM
- keep 4 cores and 4GB RAM per machine => 76 cores & 76GB
- remaining: ~7920 GB RAM, 884 cores
- 5 cores / executor => 176 executors
- memory/executor = 7920/176 = 45 GB
- minus 8% overhead => net executor memory ~41 GB

```bash
$ spark-submit --num-executors 176 --executor-memory 41g --executor-cores 5
```

## Spark

- https://itnext.io/handling-data-skew-in-apache-spark-9f56343e58e8?gi=30db4f192ca5
- https://www.geeksforgeeks.org/scala-closures/

### Serialization

- https://medium.com/@david.borsos/a-tale-of-scala-functions-and-object-serialization-9b1e890cdecb
- https://docs.google.com/viewer?a=v&pid=sites&srcid=dW10LmVkdS5wa3xiZ2RhdGF8Z3g6ODQxZDhmZjc2ZTFkZjU3
- https://stackoverflow.com/questions/22592811/task-not-serializable-java-io-notserializableexception-when-calling-function-ou
- https://medium.com/onzo-tech/serialization-challenges-with-spark-and-scala-a2287cd51c54
- https://www.nicolaferraro.me/2016/02/22/using-non-serializable-objects-in-apache-spark/
- https://stackoverflow.com/questions/6163872/how-to-serialize-a-non-serializable-in-java
- https://dzone.com/articles/what-is-serialization-everything-about-java-serial
