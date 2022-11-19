package section3

import common.readJsonDF
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object Checkpointing {
  /**
    * Boilerplate
    */
  val spark = SparkSession.builder()
    .appName("Lesson 3.3 - Checkpointing")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  /**
    * Checkpointing
    *
    * - .checkpoint() is an action
    * - Needs to be configured with sc.setCheckpointDir()
    * - CheckpointDir is usually (and recommended) a HDFS
    *
    * - Like caching
    * - Saves the RDD/DF to external storage and forgets its lineage
    * - No main memory used, only disk
    * - Takes more space and is slower than caching
    * - Disk location is usually a cluster-available file system e.g. HDFS
    * - Node failure with caching => partition is lost & needs to be recomputed
    * - Node failure with checkpointing => partition is reloaded on another executor
    * - Does not force recomputation of a partition if a node fails
    */
  def demoCheckpoint() = {
    val flights = readJsonDF(spark, "flights")

    val orderedFlights = flights.orderBy("dist")

    sc.setCheckpointDir("spark-warehouse")
    orderedFlights.checkpoint()

    val checkpointFlights = orderedFlights.checkpoint()


    orderedFlights.explain()
    /*
    == Physical Plan ==
    *(1) Sort [dist#16 ASC NULLS FIRST], true, 0
    +- Exchange rangepartitioning(dist#16 ASC NULLS FIRST, 200), true, [id=#10]
       +- FileScan json [_id#7,arrdelay#8,carrier#9,crsarrtime#10L,crsdephour#11L,crsdeptime#12L,crselapsedtime#13,depdelay#14,dest#15,dist#16,dofW#17L,origin#18] Batched: false, DataFilters: [], Format: JSON, Location: InMemoryFileIndex[file:/home/lgeniole/code/rtjvm/spark_tuning/src/main/resources/data/flights/fli..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<_id:string,arrdelay:double,carrier:string,crsarrtime:bigint,crsdephour:bigint,crsdeptime:b...
    */
    checkpointFlights.explain()
    /*
    == Physical Plan == NOTE <-- LOADS RDD FROM DISK, LOOSES LINEAGE (UNLIKE CACHING)
    *(1) Scan ExistingRDD[_id#7,arrdelay#8,carrier#9,crsarrtime#10L,crsdephour#11L,crsdeptime#12L,crselapsedtime#13,depdelay#14,dest#15,dist#16,dofW#17L,origin#18]
    */


    checkpointFlights.show()
  }


  /**
    * Cache vs. Checkpoint RDD Performance
    *
    * - Even on disk Cache wins
    * - Checkpoint is better for fault tolerance: RDD isn't recomputed on failure
    * - Checkpoint is recommended for HUGE RDDs that we can't afford to recompute
    */
  def cacheJobRDD() = {
    val number = sc.parallelize(1 to 10000000)
    val descNumbers = number.sortBy(-_).persist(StorageLevel.DISK_ONLY)

    /**
      * First .sum(): 9 seconds
      * Second .sum(): 0.4 seconds
      */
    descNumbers.sum()
    descNumbers.sum()
  }

  def checkpointJobRDD() = {
    sc.setCheckpointDir("spark-warehouse")

    val number = sc.parallelize(1 to 10000000)
    val descNumbers = number.sortBy(-_)
    descNumbers.checkpoint()

    /**
      * First .sum(): 7 seconds
      * Second .sum(): 2 seconds
      */
    descNumbers.sum()
    descNumbers.sum()
  }


  /**
    * Cache vs. Checkpoint Data Frame Performance
    *
    * - Even on disk Cache wins
    * - The performance difference is less significant
    * due to how light Data Frames are (less JVM instances)
    */
  def cacheJobDF() = {
    val flights = readJsonDF(spark, "flights")
    val orderedFlights = flights.orderBy("dist")
    orderedFlights.persist(StorageLevel.DISK_ONLY)

    /**
      * First .count(): 1 seconds
      * Second .count(): 0.2 seconds
      */
    orderedFlights.count()
    orderedFlights.count()
  }

  def checkpointJobDF() = {
    sc.setCheckpointDir("spark-warehouse")

    val flights = readJsonDF(spark, "flights")
    val orderedFlights = flights.orderBy("dist")
    val checkpointedFlights = orderedFlights.checkpoint()

    /**
      * First .count(): 0.2 seconds
      * Second .count(): 0.1 seconds
      */
    checkpointedFlights.count()
    checkpointedFlights.count()
  }


  /**
    * Conclusions
    *
    * - If a JOB is SLOW, use CACHING
    * - If a JOB is FAILING, use CHECKPOINT
    */


  def main(args: Array[String]): Unit = {
    //    demoCheckpoint()

    //    cacheJobRDD()
    //    checkpointJobRDD()

    cacheJobDF()
    checkpointJobDF()

    Thread.sleep(1000000)
  }
}

