package section4

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SizeEstimator

object ParitionsExercise {
  /**
    * Boilerplate
    */
  val spark = SparkSession.builder()
    .appName("Lesson 4.2 - Paritions Exercise")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  /**
    * Benchmark Function
    *
    * - 100000000 Long ~ 800 MB in memory
    */
  def processNumbers(newParitions: Int) = {
    val numbers = spark.range(100000000)
    val repartitioned = numbers.repartition(newParitions)

    repartitioned.cache()
    repartitioned.count()


    /**
      * This is the computation I care about
      */
    repartitioned.selectExpr("sum(id)").show()
  }


  /**
    * Benchmark Different Partition Sizes
    *
    * - Optimal job time between 40 and 400 MB  (or 10 to 100 MB) FOR UNCOMPRESSED DATA
    * - Not hard numbers, but an order of magnitude difference is not acceptable
    * - Diminishing returns with more/smaller partitions
    * - Configure default Data Frame partitions with "spark.sql.shuffle.partitions"
    * - Configure default RDD partitions with "spark.default.parallelism"
    */
  def runBenchmark(): Unit = {
    /**
      * - 400 MB / partition
      * - .show() took 0.3 seconds with 118.0 Bytes Shuffle
      */
    processNumbers(2)


    /**
      * - 40 MB / partition
      * - .show() took 0.2 seconds with 1180.0 Bytes Shuffle
      */
    processNumbers(20)


    /**
      * - 4 MB / partition
      * - .show() took 0.3 seconds with 11.5 KiB Shuffle
      */
    processNumbers(200)


    /**
      * - 400 KB / partition
      * - .show() took 2 seconds with 115.2 KiB Shuffle
      */
    processNumbers(2000)


    /**
      * - 40 KB / partition
      * - .show() took 26 seconds with 1152.3 KiB Shuffle
      */
    processNumbers(20000)
  }


  /**
    * Determining/Estimating RDD/Data Frame Size
    *
    * - Cache it and look it up in http://localhost:4040/storage
    *   - Actual memory/disk usage
    *   - Might be serialized or compressed (Data Set)
    *   - Good accuracy
    *
    * - Use SizeEstimator.estimate()
    *   - Estimates the memory footprint of JVM objects
    *   - Accuracy within an order of magnitude (usually larger)
    *   - Result in bytes
    *
    * - Use a Query Plan
    *   - Most accurate for the DATA
    *   - Result in bytes
    */
  def dfSizeEstimator() = {
    val numbers = spark.range(100000)
    println(SizeEstimator.estimate(numbers))
    /*
    5891448
     */

    numbers.cache()
    /*
    101.5 KiB
     */
    numbers.count()
  }


  def estimateWithQueryPlan() = {
    val numbers = spark.range(100000)
    println(numbers.queryExecution.optimizedPlan.stats.sizeInBytes)
    /*
    800000
     */
  }


  def estimateRDD() = {
    val numbers = sc.parallelize(1 to 100000)
    numbers.cache().count()
  }


  def main(args: Array[String]): Unit = {
    //    runBenchmark()

    //    dfSizeEstimator()
    //    estimateWithQueryPlan()
    estimateRDD()

    Thread.sleep(1000000)
  }
}
