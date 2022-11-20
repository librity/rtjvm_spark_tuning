package section5

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object KryoSerializer {
  /**
    * 1. Define a sparkConf object with the Kryo Serializer
    * 2. Register the classes we want to Serialize
    * (not necessary but massively improves performance)
    */
  val sparkConf = new SparkConf()
    .set("spark.serializer", "org.apache.serializer.KryoSerializer")
    .set("spark.kryo.registrationRequired", "true")
    .registerKryoClasses(Array(
      classOf[Person],
      classOf[Array[Person]],
    ))


  /**
    * Boilerplate
    */
  val spark = SparkSession.builder()
    .appName("Lesson 5.5 - Kryo Serializer")
    .config(sparkConf)
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  /**
    * Generate People
    */
  case class Person(name: String, age: Int)

  def generatePeople(count: Int) = (1 to count).map(i => Person(s"Person$i", i % 100))

  val people = sc.parallelize(generatePeople(10000000))

  /**
    * Using Java's Serializer:
    * - Time: 30 seconds
    * - Memory Usage: 254.1 MiB
    *
    * Using the Kryo Serializer:
    * - Once configured runs automatically
    * - Time: 22 seconds
    * - Memory Usage: 164.5 MiB
    */
  def testCaching() = people.persist(StorageLevel.MEMORY_ONLY_SER).count()


  /**
    * Using Java's Serializer:
    * - Time: 29 seconds
    * - Shuffle: 74.2 MiB
    *
    * Using the Kryo Serializer:
    * - Time: 20 seconds
    * - Shuffle: 43.0 MiB
    */
  def testShuffling() = people
    .map(person => (person.age, person))
    .groupByKey()
    .mapValues(_.size)
    .count()


  def main(args: Array[String]): Unit = {
    //    testCaching()
    testShuffling()

    Thread.sleep(1000000)
  }
}
