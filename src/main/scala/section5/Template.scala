package section5

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import common.{buildJsonPath, inspect, readJsonDF}

object Template {
  /**
    * Boilerplate
    */

  val spark = SparkSession.builder()
    .appName("Lesson  - ")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  val sc = spark.sparkContext
  sc.setLogLevel("WARN")


  def main(args: Array[String]): Unit = {

  }
}
