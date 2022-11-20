package section5

import common.DataGenerator.{randomGuitar, randomGuitarSale}
import common.{Guitar, GuitarSale}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._

object FixingDataSkews {
  /**
    * Boilerplate
    *
    * - Deactivate Broadcast Joins
    */
  val spark = SparkSession.builder()
    .appName("Lesson 5.2 - Fixing Data Skews")
    .master("local[*]")
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  /**
    * Generate Data
    */
  val guitars: Dataset[Guitar] = Seq.fill(40000)(randomGuitar()).toDS()
  val guitarSales: Dataset[GuitarSale] = Seq.fill(200000)(randomGuitarSale()).toDS()


  /**
    * A Guitar is similar to a GuitarSale if:
    * - Same make and model
    * - abs(guitar.soundScore - guitarSale.soundScore) < 0.1
    *
    * Problem:
    * - For every guitar, avg(sale price of all similar GuitarSales)
    * - Gibson L-00, config "sdasde", sound 4.3,
    * compute avg(sale prices of all GuitarSales of Gibson L-00 with 4.2 <= soundQuality <= 4.4)
    */


  /**
    * Naive Solution
    *
    * - Took 1.4 minutes with a Straggling task of 1.3 minutes
    * - Data is skewed and one job gets most of the workload in the join (Stage 2)
    */
  def naiveJoin() = {
    val joined = guitars
      .join(guitarSales, Seq("make", "model"))
      .where(
        abs(guitarSales("soundScore") - guitars("soundScore")) <= 0.1
      )
      .groupBy("configurationId")
      .agg(avg("salePrice").as("averagePrice"))


    joined.explain()
    /*
    == Physical Plan ==
    *(4) HashAggregate(keys=[configurationId#4], functions=[avg(salePrice#22)])
    +- Exchange hashpartitioning(configurationId#4, 200), true, [id=#45]
       +- *(3) HashAggregate(keys=[configurationId#4], functions=[partial_avg(salePrice#22)])
          +- *(3) Project [configurationId#4, salePrice#22]
             +- *(3) SortMergeJoin [make#5, model#6], [make#19, model#20], Inner, (abs((soundScore#21 - soundScore#7)) <= 0.1)
                :- *(1) Sort [make#5 ASC NULLS FIRST, model#6 ASC NULLS FIRST], false, 0
                :  +- Exchange hashpartitioning(make#5, model#6, 200), true, [id=#23]
                :     +- LocalTableScan [configurationId#4, make#5, model#6, soundScore#7]
                +- *(2) Sort [make#19 ASC NULLS FIRST, model#20 ASC NULLS FIRST], false, 0
                   +- Exchange hashpartitioning(make#19, model#20, 200), true, [id=#24]
                      +- LocalTableScan [make#19, model#20, soundScore#21, salePrice#22]
    */


    joined.count()
  }


  /**
    * Salted Deskew
    *
    * - Make and model is unevenly distributed
    * - Salting: Add random data so to distribute it more evenly
    * - Create a salt column with an even distribution
    * - Multiply guitars row count by 100
    * - 0 to 99 Is the salting interval
    * - Add an evenly-distributed salt to guitarSales
    *
    * - Took 31 seconds with a maximum task duration of 6 seconds in the join (Stage 2)
    * - For tuning, adjust and test salting interval
    */
  def saltedJoin() = {
    val saltInterval = (0 to 99).toArray
    val saltSize = saltInterval.length
    //  println(s"saltInterval: $saltInterval")
    //  println(s"saltSize: $saltSize")


    val saltedGuitars = guitars
      .withColumn(
        "salt",
        explode(lit(saltInterval))
      )
    val saltedGuitarSales = guitarSales
      .withColumn(
        "salt",
        monotonically_increasing_id() % saltSize,
      )

    val deskewedJoin = saltedGuitars.join(
      saltedGuitarSales,
      Seq("make", "model", "salt")
    )
      .where(
        abs(saltedGuitarSales("soundScore") - saltedGuitars("soundScore")) <= 0.1
      )
      .groupBy("configurationId")
      .agg(avg("salePrice").as("averagePrice"))


    deskewedJoin.explain()
    /*
    == Physical Plan ==
    *(5) HashAggregate(keys=[configurationId#4], functions=[avg(salePrice#22)])
    +- Exchange hashpartitioning(configurationId#4, 200), true, [id=#57]
       +- *(4) HashAggregate(keys=[configurationId#4], functions=[partial_avg(salePrice#22)])
          +- *(4) Project [configurationId#4, salePrice#22]
             +- *(4) SortMergeJoin [make#5, model#6, cast(salt#30 as bigint)], [make#19, model#20, salt#36L], Inner, (abs((soundScore#21 - soundScore#7)) <= 0.1)
                :- *(2) Sort [make#5 ASC NULLS FIRST, model#6 ASC NULLS FIRST, cast(salt#30 as bigint) ASC NULLS FIRST], false, 0
                :  +- Exchange hashpartitioning(make#5, model#6, cast(salt#30 as bigint), 200), true, [id=#44]
                :     +- *(1) Filter isnotnull(salt#30)
                :        +- Generate explode([0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94,95,96,97,98,99]), [configurationId#4, make#5, model#6, soundScore#7], false, [salt#30]
                :           +- LocalTableScan [configurationId#4, make#5, model#6, soundScore#7]
                +- *(3) Sort [make#19 ASC NULLS FIRST, model#20 ASC NULLS FIRST, salt#36L ASC NULLS FIRST], false, 0
                   +- Exchange hashpartitioning(make#19, model#20, salt#36L, 200), true, [id=#32]
                      +- LocalTableScan [make#19, model#20, soundScore#21, salePrice#22, salt#36L]
    */



    deskewedJoin.count()
  }


  def main(args: Array[String]): Unit = {
    //  naiveJoin()
    saltedJoin()

    Thread.sleep(1000000)
  }
}
