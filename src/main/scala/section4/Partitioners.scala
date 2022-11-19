package section4

import org.apache.spark.{HashPartitioner, Partitioner, RangePartitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.util.Random

object Partitioners {
  /**
    * Boilerplate
    */
  val spark = SparkSession.builder()
    .appName("Lesson 4.2 - Partitioners")
    .master("local[1]")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  /**
    *
    * - Controlling the partitioning scheme is only possible on RDDs
    * - No partitioner logic
    * - numbers.partitioner == None
    */
  //  val numbers = sc.parallelize(1 to 10000)
  //  println(numbers.partitioner)


  /**
    * - Random data redistribution
    * - numbers3.partitioner == None
    */
  //  val numbers3 = numbers.repartition(3)
  //  println(numbers3.partitioner)


  /**
    *
    * - Keep track of the partitioner (GOOD PRACTICE)
    * - Especially for KeyValue RDDs: we can control partitioning scheme and OPTIMIZE
    */
  //  val keyedNumbers: RDD[(Int, Int)] = numbers.map { n => (n % 10, n) }


  /**
    * HashPartitioner(partitions)
    *
    * - All keys with the same hash stay on the same partition
    * - Prerequisite for all .xByKey() operations
    * - .combineByKey(), .groupByKey(), .aggregateByKey(), .foldByKey(), .reduceByKey()
    * - Prerequisite for a join when neither RDD has a know Partitioner (rdd.partitioner == None)
    */
  //  val hashedNumbers = keyedNumbers.partitionBy(new HashPartitioner(4))


  /**
    * RangePartitioner(partitions, rdd)
    *
    * - Sample keys to determine $partitions equal interval to redistribute
    * - Keys in the same range will be in the same partition
    * - 5 partition for 0-1000: Int.MinValue-200 will be the first partition, 200-400 will be the second, etc.
    * - Keys are naturally ordered, prerequisite for a sort
    * - rangedNumbers.sortByKey() does not incur a Shuffle!
    */
  //  val rangedNumbers = keyedNumbers.partitionBy(new RangePartitioner(5, keyedNumbers))
  //  rangedNumbers.sortByKey()


  /**
    * Custom Partitioners (ADVANCED)
    */
  def generateRandomWords(count: Int, maxLenght: Int) = {
    val random = new Random()

    (1 to count).map { _ =>
      random.alphanumeric.take(
        random.nextInt(maxLenght)
      ).mkString("")
    }
  }

  val randomWordsRDD = sc.parallelize(generateRandomWords(1000, 100))


  /**
    * Repartition RDD by Word Length
    *
    * - Two words of the same length will be on the same partition
    * - Custom computation: count occurrence of "z" in every word
    * - 100 Partitions: because zWords max length is 100
    * - getPartition: Returns partition index
    */
  val zWordsRDD: RDD[(String, Int)] = randomWordsRDD.map { word =>
    (word, word.count(_ == 'z'))
  }

  class ByLengthPartitioner(override val numPartitions: Int) extends Partitioner {
    override def getPartition(key: Any): Int = key.toString.length % numPartitions
  }

  val zWordsByLength = zWordsRDD.partitionBy(new ByLengthPartitioner(100))


  /**
    * Conclusions
    *
    * - Create your own Partitioners when Spark's defaults are too slow or inappropriate
    * - Can have massive performance impact
    */

  def main(args: Array[String]): Unit = {
    /**
      * .master("local[1]"): We only use one executor to print partitions in order
      */
    zWordsByLength.foreachPartition(_.foreach(println))
  }
}
