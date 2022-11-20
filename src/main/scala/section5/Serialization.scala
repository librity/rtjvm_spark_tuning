package section5

import org.apache.spark.sql.SparkSession

object Serialization {
  /**
    * Boilerplate
    */
  val spark = SparkSession.builder()
    .appName("Lesson 5.3 - Serialization")
    .master("local[*]")
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  /**
    * Basic RDD Transformation
    *
    * - Works as you'd expect
    */
  val rdd = sc.parallelize(1 to 100)

  class RDDMultiplier {
    def multiplyRDD(): List[Int] = rdd.map(_ * 2).collect().toList
  }

  val rddMultiplier = new RDDMultiplier
  //  rddMultiplier.multiplyRDD()


  /**
    * More general transformation
    *
    * - Doesn't work: Throws task not serializable error
    * - Needs to serialize and send moreGeneralRddMultiplier to all the executors
    * - MoreGeneralRDDMultiplier isn't Serializable by default
    */
  class MoreGeneralRDDMultiplier {
    val factor = 2

    def multiplyRDD(): List[Int] = rdd.map(_ * factor).collect().toList
  }

  val moreGeneralRddMultiplier = new MoreGeneralRDDMultiplier
  //  moreGeneralRddMultiplier.multiplyRDD()


  /**
    * Serialize Class With Trait
    *
    * - Pro: Easy
    * - Con: Not every class is serializable with this trait
    * - Con: Serializing and broadcasting a class across the cluster has an overhead
    */
  class SerializableRDDMultiplier extends Serializable {
    val factor = 2

    def multiplyRDD(): List[Int] = rdd.map(_ * factor).collect().toList
  }

  val serializableRddMultiplier = new SerializableRDDMultiplier
  //  serializableRddMultiplier.multiplyRDD()


  /**
    * Enclose Member In Local Value
    *
    * -
    * - We have to be careful with closures due to the distributed nature of Spark
    */
  class EnclosedRDDMultiplier {
    val factor = 2

    def multiplyRDD(): List[Int] = {
      val enclosedFactor = factor

      rdd.map(_ * enclosedFactor).collect().toList
    }
  }

  val enclosedRddMultiplier = new EnclosedRDDMultiplier
  //  enclosedRddMultiplier.multiplyRDD()


  /**
    * Exercise 1
    *
    * - Will this work? No
    * - Why? Because of the factor
    */
  class NestedClassRDDMultiplier {
    val factor = 2

    object NestedMultiplier extends Serializable {
      val extraTerm = 10

      def multiplyRDD(): List[Int] = rdd.map(_ * factor + extraTerm).collect().toList
    }
  }
  //  (new NestedClassRDDMultiplier).NestedMultiplier.multiplyRDD()

  /**
    * Solution 1
    *
    * - Enclose/capture factor
    */
  class WorkingNestedClassRDDMultiplierV1 {
    val factor = 2

    object NestedMultiplier extends Serializable {
      val extraTerm = 10
      val capturedFactor = factor

      def multiplyRDD(): List[Int] = rdd.map(_ * capturedFactor + extraTerm).collect().toList
    }
  }

  //  (new WorkingNestedClassRDDMultiplierV1).NestedMultiplier.multiplyRDD()


  /**
    * Solution 2
    *
    * - Make it Serializable
    */
  class WorkingNestedClassRDDMultiplierV2 extends Serializable {
    val factor = 2

    object NestedMultiplier extends Serializable {
      val extraTerm = 10

      def multiplyRDD(): List[Int] = rdd.map(_ * factor + extraTerm).collect().toList
    }
  }

  //  (new WorkingNestedClassRDDMultiplierV2).NestedMultiplier.multiplyRDD()


  /**
    * This Won't Work
    *
    * - Look at the stack trace
    * - BrokenNestedClassRDDMultiplier isn't serializable
    * - enclosedFactor references $outer.factor, accessing the outer class in a hidden way
    */
  class BrokenNestedClassRDDMultiplier {
    val factor = 2

    object NestedMultiplier extends Serializable {
      val extraTerm = 10

      def multiplyRDD(): List[Int] = {
        val enclosedFactor = factor

        rdd.map(_ * enclosedFactor + extraTerm).collect().toList
      }
    }
  }

  //  (new BrokenNestedClassRDDMultiplier).NestedMultiplier.multiplyRDD()


  /**
    * Exercise 2
    *
    * - Will this work? No
    * - Why? DrinkingAgeChecker isn't Serializable
    */
  case class Person(name: String, age: Int)

  val people = sc.parallelize(List(
    Person("Alice", 43),
    Person("Bob", 12),
    Person("Charlie", 23),
    Person("Diana", 67),
  ))

  class DrinkingAgeChecker(legalAge: Int) {
    def processPeople(): List[Boolean] = people.map(_.age >= legalAge).collect().toList
  }

  //  (new DrinkingAgeChecker(21)).processPeople()

  /**
    * Solution 1
    *
    * - Make it Serializable
    */
  class WorkingDrinkingAgeCheckerV1(legalAge: Int) extends Serializable {
    def processPeople(): List[Boolean] = people.map(_.age >= legalAge).collect().toList
  }

  //  (new WorkingDrinkingAgeCheckerV1(21)).processPeople()

  /**
    * Solution 2
    *
    * - Capture legalAge
    */
  class WorkingDrinkingAgeCheckerV2(legalAge: Int) {
    def processPeople(): List[Boolean] = {
      val capturedLegalAge = legalAge

      people.map(_.age >= capturedLegalAge).collect().toList
    }
  }

  //  (new WorkingDrinkingAgeCheckerV2(21)).processPeople()


  /**
    *
    */


  /**
    *
    */


  /**
    *
    */


  /**
    *
    */


  /**
    *
    */


  /**
    *
    */


  /**
    *
    */


  /**
    *
    */

  def main(args: Array[String]): Unit = {

  }
}
