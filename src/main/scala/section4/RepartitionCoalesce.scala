package section4

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object RepartitionCoalesce {
  /**
    * Boilerplate
    */
  val spark = SparkSession.builder()
    .appName("Lesson 4.1 - Repartition Coalesce")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  /**
    * Create RDD
    *
    *  - For .master("local[*]"), the number of partitions
    *    will be the number of virtual cores (8 in my case)
    */
  val numbers = sc.parallelize(1 to 10000000)
  println(numbers.partitions.length)


  /**
    * .repartition()
    *
    * - .count() took 3 seconds with a 76.3 MiB Shuffle
    */
  val repartitionedNumbers = numbers.repartition(2)
  println(repartitionedNumbers.toDebugString)
  /*
  (2) MapPartitionsRDD[4] at repartition at RepartitionCoalesce.scala:39 []
   |  CoalescedRDD[3] at repartition at RepartitionCoalesce.scala:39 []
   |  ShuffledRDD[2] at repartition at RepartitionCoalesce.scala:39 []
   +-(8) MapPartitionsRDD[1] at repartition at RepartitionCoalesce.scala:39 []
      |  ParallelCollectionRDD[0] at parallelize at RepartitionCoalesce.scala:29 []
   */
  repartitionedNumbers.count()


  /**
    * .coalesce()
    *
    * - Fundamentally different to .repartition()
    * - Also changes the RDD's number of partitions
    * - .count() took 0.2 seconds with NO Shuffle (in this case)
    */
  val coalescedNumbers = numbers.coalesce(2)
  println(coalescedNumbers.toDebugString)
  /*
  (2) CoalescedRDD[5] at coalesce at RepartitionCoalesce.scala:53 []
   |  ParallelCollectionRDD[0] at parallelize at RepartitionCoalesce.scala:29 []
   */
  coalescedNumbers.count()


  /**
    * Repartition vs. Coalesce
    *
    * - Repartition is a Full Shuffle that redistributes the data evenly
    * - Dependencies are wide
    *
    * - Coalesce "stitches" existing partitions together
    * - Dependencies are narrow
    * - Almost always faster than a Shuffle
    */


  /**
    * Force .coalesce() to Shuffle
    *
    * - .repartition() is implemented in terms of .coalesce()
    */
  val forcedShuffle = numbers.coalesce(2, true)
  println(forcedShuffle.toDebugString)
  /*
  (2) MapPartitionsRDD[9] at coalesce at RepartitionCoalesce.scala:81 []
     |  CoalescedRDD[8] at coalesce at RepartitionCoalesce.scala:81 []
     |  ShuffledRDD[7] at coalesce at RepartitionCoalesce.scala:81 []
     +-(8) MapPartitionsRDD[6] at coalesce at RepartitionCoalesce.scala:81 []
        |  ParallelCollectionRDD[0] at parallelize at RepartitionCoalesce.scala:27 []
   */
  forcedShuffle.count()


  /**
    * Conclusions
    *
    * - Use REPARTITION to INCREASE PARALLELISM and DISTRIBUTE data EVENLY
    * - Use COALESCE to REDUCE PARTITION NUMBER and DISTRIBUTE data ARBITRARILY
    */

  def main(args: Array[String]): Unit = {
    Thread.sleep(1000000)
  }
}
