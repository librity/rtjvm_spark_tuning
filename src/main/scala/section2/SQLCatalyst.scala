package section2

import common.{buildDataPath, readJsonDF}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SQLCatalyst {
  /**
    * Boilerplate
    */
  val spark = SparkSession.builder()
    .appName("Lesson 2.7 - SQL Catalyst")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  /**
    * Load Data Frames
    */
  val flights = readJsonDF(spark, "flights")
  //  flights.show()


  /**
    * Filter Chaining
    */
  val chainedFilters = flights
    .where($"origin" =!= "LGA")
    .where($"origin" =!= "ORD")
    .where($"origin" =!= "SFO")
    .where($"origin" =!= "DEN")
    .where($"origin" =!= "BOS")
    .where($"origin" =!= "EWR")


  //  chainedFilters.explain(true)
  /*
  == Parsed Logical Plan ==
  'Filter NOT ('origin = EWR)
  +- Filter NOT (origin#18 = BOS)
     +- Filter NOT (origin#18 = DEN)
        +- Filter NOT (origin#18 = SFO)
           +- Filter NOT (origin#18 = ORD)
              +- Filter NOT (origin#18 = LGA)
                 +- Relation[_id#7,arrdelay#8,carrier#9,crsarrtime#10L,crsdephour#11L,crsdeptime#12L,crselapsedtime#13,depdelay#14,dest#15,dist#16,dofW#17L,origin#18] json

  == Analyzed Logical Plan ==
  _id: string, arrdelay: double, carrier: string, crsarrtime: bigint, crsdephour: bigint, crsdeptime: bigint, crselapsedtime: double, depdelay: double, dest: string, dist: double, dofW: bigint, origin: string
  Filter NOT (origin#18 = EWR)
  +- Filter NOT (origin#18 = BOS)
     +- Filter NOT (origin#18 = DEN)
        +- Filter NOT (origin#18 = SFO)
           +- Filter NOT (origin#18 = ORD)
              +- Filter NOT (origin#18 = LGA)
                 +- Relation[_id#7,arrdelay#8,carrier#9,crsarrtime#10L,crsdephour#11L,crsdeptime#12L,crselapsedtime#13,depdelay#14,dest#15,dist#16,dofW#17L,origin#18] json

  == Optimized Logical Plan ==
  Filter ((((((isnotnull(origin#18) AND NOT (origin#18 = LGA)) AND NOT (origin#18 = ORD)) AND NOT (origin#18 = SFO)) AND NOT (origin#18 = DEN)) AND NOT (origin#18 = BOS)) AND NOT (origin#18 = EWR))
  +- Relation[_id#7,arrdelay#8,carrier#9,crsarrtime#10L,crsdephour#11L,crsdeptime#12L,crselapsedtime#13,depdelay#14,dest#15,dist#16,dofW#17L,origin#18] json

  == Physical Plan == NOTE <-- Filters are chained together in a single Project (GOOD)
  *(1) Project [_id#7, arrdelay#8, carrier#9, crsarrtime#10L, crsdephour#11L, crsdeptime#12L, crselapsedtime#13, depdelay#14, dest#15, dist#16, dofW#17L, origin#18]
  +- *(1) Filter ((((((isnotnull(origin#18) AND NOT (origin#18 = LGA)) AND NOT (origin#18 = ORD)) AND NOT (origin#18 = SFO)) AND NOT (origin#18 = DEN)) AND NOT (origin#18 = BOS)) AND NOT (origin#18 = EWR))
     +- FileScan json [_id#7,arrdelay#8,carrier#9,crsarrtime#10L,crsdephour#11L,crsdeptime#12L,crselapsedtime#13,depdelay#14,dest#15,dist#16,dofW#17L,origin#18] Batched: false, DataFilters: [isnotnull(origin#18), NOT (origin#18 = LGA), NOT (origin#18 = ORD), NOT (origin#18 = SFO), NOT (..., Format: JSON, Location: InMemoryFileIndex[file:/home/lgeniole/code/rtjvm/spark_tuning/src/main/resources/data/flights/fli..., PartitionFilters: [], PushedFilters: [IsNotNull(origin), Not(EqualTo(origin,LGA)), Not(EqualTo(origin,ORD)), Not(EqualTo(origin,SFO)),..., ReadSchema: struct<_id:string,arrdelay:double,carrier:string,crsarrtime:bigint,crsdephour:bigint,crsdeptime:b...
  */


  /**
    * Duplicate Filters
    */
  def filterFromTeam1(flights: DataFrame) = flights
    .where($"origin" =!= "LGA")
    .where($"dest" === "DEN")

  def filterFromTeam2(flights: DataFrame) = flights
    .where($"origin" =!= "EWR")
    .where($"dest" === "DEN")

  val finalFilter = filterFromTeam1(filterFromTeam2(flights))

  //  finalFilter.explain(true)
  /*
  == Parsed Logical Plan ==
  'Filter ('dest = DEN)
  +- Filter NOT (origin#18 = LGA)
     +- Filter (dest#15 = DEN)
        +- Filter NOT (origin#18 = EWR)
           +- Relation[_id#7,arrdelay#8,carrier#9,crsarrtime#10L,crsdephour#11L,crsdeptime#12L,crselapsedtime#13,depdelay#14,dest#15,dist#16,dofW#17L,origin#18] json

  == Analyzed Logical Plan ==
  _id: string, arrdelay: double, carrier: string, crsarrtime: bigint, crsdephour: bigint, crsdeptime: bigint, crselapsedtime: double, depdelay: double, dest: string, dist: double, dofW: bigint, origin: string
  Filter (dest#15 = DEN)
  +- Filter NOT (origin#18 = LGA)
     +- Filter (dest#15 = DEN)
        +- Filter NOT (origin#18 = EWR)
           +- Relation[_id#7,arrdelay#8,carrier#9,crsarrtime#10L,crsdephour#11L,crsdeptime#12L,crselapsedtime#13,depdelay#14,dest#15,dist#16,dofW#17L,origin#18] json

  == Optimized Logical Plan == NOTE <-- Removes duplicate filters (GOOD)
  Filter ((((isnotnull(dest#15) AND isnotnull(origin#18)) AND NOT (origin#18 = EWR)) AND (dest#15 = DEN)) AND NOT (origin#18 = LGA))
  +- Relation[_id#7,arrdelay#8,carrier#9,crsarrtime#10L,crsdephour#11L,crsdeptime#12L,crselapsedtime#13,depdelay#14,dest#15,dist#16,dofW#17L,origin#18] json

  == Physical Plan ==
  *(1) Project [_id#7, arrdelay#8, carrier#9, crsarrtime#10L, crsdephour#11L, crsdeptime#12L, crselapsedtime#13, depdelay#14, dest#15, dist#16, dofW#17L, origin#18]
  +- *(1) Filter ((((isnotnull(dest#15) AND isnotnull(origin#18)) AND NOT (origin#18 = EWR)) AND (dest#15 = DEN)) AND NOT (origin#18 = LGA))
     +- FileScan json [_id#7,arrdelay#8,carrier#9,crsarrtime#10L,crsdephour#11L,crsdeptime#12L,crselapsedtime#13,depdelay#14,dest#15,dist#16,dofW#17L,origin#18] Batched: false, DataFilters: [isnotnull(dest#15), isnotnull(origin#18), NOT (origin#18 = EWR), (dest#15 = DEN), NOT (origin#18..., Format: JSON, Location: InMemoryFileIndex[file:/home/lgeniole/code/rtjvm/spark_tuning/src/main/resources/data/flights/fli..., PartitionFilters: [], PushedFilters: [IsNotNull(dest), IsNotNull(origin), Not(EqualTo(origin,EWR)), EqualTo(dest,DEN), Not(EqualTo(ori..., ReadSchema: struct<_id:string,arrdelay:double,carrier:string,crsarrtime:bigint,crsdephour:bigint,crsdeptime:b...
  */


  /**
    * Pushing Down Filters
    */
  val parquetPath = buildDataPath("flights.parquet")
  flights.write.mode(SaveMode.Overwrite).save(parquetPath)

  val notFromLGA = spark.read.load(parquetPath)
    .where($"origin" =!= "LGA")

  notFromLGA.explain
  /*
  == Physical Plan ==
  *(1) Project [_id#55, arrdelay#56, carrier#57, crsarrtime#58L, crsdephour#59L, crsdeptime#60L, crselapsedtime#61, depdelay#62, dest#63, dist#64, dofW#65L, origin#66]
  +- *(1) Filter (isnotnull(origin#66) AND NOT (origin#66 = LGA))
     +- *(1) ColumnarToRow
        +- FileScan parquet [_id#55,arrdelay#56,carrier#57,crsarrtime#58L,crsdephour#59L,crsdeptime#60L,crselapsedtime#61,depdelay#62,dest#63,dist#64,dofW#65L,origin#66] Batched: true, DataFilters: [isnotnull(origin#66), NOT (origin#66 = LGA)], Format: Parquet, Location: InMemoryFileIndex[file:/home/lgeniole/code/rtjvm/spark_tuning/src/main/resources/data/flights.par..., PartitionFilters: [],
           PushedFilters: [IsNotNull(origin), Not(EqualTo(origin,LGA))], NOTE <-- Catalyst Filters straight from the source (GOOD)
           ReadSchema: struct<_id:string,arrdelay:double,carrier:string,crsarrtime:bigint,crsdephour:bigint,crsdeptime:b...
  */

  def main(args: Array[String]): Unit = {

  }
}
