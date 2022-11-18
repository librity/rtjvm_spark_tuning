package section2

import org.apache.spark.sql.SparkSession

object Tungsten {
  /**
    * Boilerplate
    */
  val spark = SparkSession.builder()
    .appName("Lesson 2.8 - ")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  /**
    * Spark Tungsten
    *
    * - Bypasses the JVM to do some low-level optimizations on CPU and memory
    */


  /**
    * .cache()
    *
    * - Saves RDD in memory so we can retrieve it later
    * - Lazy evaluated, triggered at first .count() (action), taking 1 second
    * - Retrieved at second .count(), taking 0.1 seconds (much faster)
    * - In the Web View's Storage tab:
    * 0	ParallelCollectionRDD	Memory Deserialized 1x Replicated	8	100%	38.1 MiB	0.0 B
    */
  val numbersRDD = sc.parallelize(1 to 10000000).cache()
  //  numbersRDD.count()
  //  numbersRDD.count()

  /**
    * - Data Frames are cached with Tungsten
    * - Even faster than RDD caching
    * - First count: 1 second
    * - Second count: 41 milliseconds (ROCKET)
    * - In the Web View's Storage tab:
    * 3	*(1) Project [value#2 AS id#5] +- *(1) SerializeFromObject [input[0, int, false] AS value#2] +- Scan[obj#1]	Disk Memory Deserialized 1x Replicated	8	100%	9.7 MiB	0.0 B
    * - Less Size in Memory (9.7 MiB), not a JVM object, optimized storage
    * - WholeStageCodegen in the DAG == Tungsten
    */
  val numbersDF = numbersRDD.toDF("id").cache()
  //  numbersDF.count()
  //  numbersDF.count()


  /**
    * - We can deactivate Tungsten on the fly
    * - First job took 0.4 seconds
    * - First job took 96 milliseconds
    * - Tasks with an asterisk * in the Physical plan mean that Spark is using Tungsten
    */
  spark.conf.set("spark.sql.codegen.wholeStage", "false")
  val noWholeStageSum = spark.range(1000000).selectExpr("sum(id)")
  noWholeStageSum.explain()
  /*
  == Physical Plan ==
  HashAggregate(keys=[], functions=[sum(id#54L)])
  +- Exchange SinglePartition, true, [id=#73]
     +- HashAggregate(keys=[], functions=[partial_sum(id#54L)])
        +- Range (0, 1000000, step=1, splits=8)
  */
  noWholeStageSum.show()


  spark.conf.set("spark.sql.codegen.wholeStage", "true")
  val wholeStageSum = spark.range(1000000).selectExpr("sum(id)")
  wholeStageSum.explain()
  /*
  == Physical Plan ==
  *(2) HashAggregate(keys=[], functions=[sum(id#66L)])
  +- Exchange SinglePartition, true, [id=#104]
     +- *(1) HashAggregate(keys=[], functions=[partial_sum(id#66L)])
        +- *(1) Range (0, 1000000, step=1, splits=8)
  */
  wholeStageSum.show()


  def main(args: Array[String]): Unit = {
    Thread.sleep(1000000)
  }
}
