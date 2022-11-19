package section3

import common.readJsonDF
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

object Caching {
  /**
    * Boilerplate
    */
  val spark = SparkSession.builder()
    .appName("Lesson 3.2 - Caching")
    .master("local[*]")

    // StorageLevel.OFF_HEAP configs
    .config("spark.memory.offHeap.enabled", "true")
    // Size in bytes
    .config("spark.memory.offHeap.size", 100000000)

    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  /**
    * - Caching is done by executors on partitions
    * - Caching too much can be as bad as caching too little
    * - More caching -> More JVM memory heap -> More garbage collection -> Bad performance
    * - Minimum JVM memory: 4-8GB
    * - Maximum JVM memory: 40GB
    */


  /**
    * Load Data Frame
    */
  val flights = readJsonDF(spark, "flights")
  //flights.count()


  /**
    * Simulate an expensive operation
    *
    * - Cache Data Frames that will be reused often
    * - dataFrame.cache(): Caches and returns the dataFrame
    * Uses .persist() (more general version of caching)
    * - Without .cache(): 0.4 seconds
    * - .cache() and .count(): 0.9 seconds
    * - With .cache(): 0.1 seconds
    * - http://localhost:4040/storage/
    * ID	RDD Name	Storage Level	Cached Partitions	Fraction Cached	Size in Memory	Size on Disk
    * 19	*Sort(...	Disk Memory Deserialized 1x Replicated	35	100%	3.0 MiB	0.0 B
    */
  val orderedFlights = flights.orderBy("dist")

  //  orderedFlights.count()
  //  orderedFlights.cache().count()
  //  orderedFlights.count()


  /**
    * .persist()
    *
    * - StorageLevel.MEMORY_AND_DISK (Default):
    *   - Cache in the heap and in the disk
    *   - if it is Evicted from Executor's storage memory (usually in a Shuffle).
    *   - Good tradeoff between MEMORY_ONLY and DISK_ONLY
    *
    * - StorageLevel.MEMORY_ONLY:
    *   - Cache in the memory with no serialization
    *   - CPU efficient but memory expensive
    *
    * - StorageLevel.DISK_ONLY:
    *   - Cache to disk
    *   - CPU and memory efficient but slower
    *
    * - StorageLevel.MEMORY_ONLY_SER:
    *   - Cache serialized to memory
    *   - CPU intensive and memory efficient
    *   - Not impactful for Data Frames but impactful for RDDs
    *   - We can also use _SER suffix for the other Storage Levels
    *
    * - StorageLevel.MEMORY_ONLY_2:
    *   - Cache to memory replicated twice for resiliency
    *   - 2x memory usage
    *   - We can also use _2 suffix for the other Storage Levels
    *
    * - StorageLevel.OFF_HEAP:
    *   - Cache outside the JVM, on the machine RAM with Tungsten
    *   - Needs to be configured in the constructor
    *   - CPU and memory efficient
    *
    */
  //  orderedFlights.persist().count()
  //  orderedFlights.persist(StorageLevel.MEMORY_AND_DISK).count()
  //  orderedFlights.persist(StorageLevel.MEMORY_ONLY).count()
  //  orderedFlights.persist(StorageLevel.DISK_ONLY).count()
  //  orderedFlights.persist(StorageLevel.MEMORY_ONLY_SER).count()
  orderedFlights.persist(StorageLevel.OFF_HEAP).count()


  /**
    * .unpersist()
    *
    * - Removes from cache
    */
  orderedFlights.unpersist()


  /**
    * spark.catalog.cacheTable()
    *
    * - Caches the table with a custom name
    * - Looks better in http://localhost:4040/storage/
    */
  orderedFlights.createOrReplaceTempView("ordered_flights")
  spark.catalog.cacheTable("ordered_flights")
  orderedFlights.count()


  /**
    * Caching RDDs
    *
    * - Unserialized RDDs take way more memory than Data Frames
    * - StorageLevel.MEMORY_ONLY: 17.9 MiB
    * - StorageLevel.MEMORY_ONLY_SER: 7.0 MiB
    */
  val flightsRDD = orderedFlights.rdd
  //  flightsRDD.persist(StorageLevel.MEMORY_ONLY)
  flightsRDD.persist(StorageLevel.MEMORY_ONLY_SER)
  flightsRDD.count()


  def main(args: Array[String]): Unit = {
    Thread.sleep(1000000)
  }
}
