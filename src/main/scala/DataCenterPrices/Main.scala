package DataCenterPrices

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, DoubleType, StringType, IntegerType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import scala.util.matching.Regex
import java.nio.file.Paths
import java.io.File
import java.io.PrintWriter

object Main {
  // args[0]: path of the input dir
  def main(args: Array[String]): Unit = {
    // Step 1: Create Spark session
    val spark = SparkSession.builder.appName("Impact of Data Centers on Residential Electricity Prices").getOrCreate()

    // Import implicits like $"columnName" after SparkSession creation
    import spark.implicits._


    // Step N-1: Save output to disk

    // Step N: Stop the spark session
    spark.stop()

  }
}