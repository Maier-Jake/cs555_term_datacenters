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
  def main(args: Array[String]): Unit = {
    // Step 1: Create Spark session
    val spark = SparkSession.builder.appName("Impact of Data Centers on Residential Electricity Prices").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    println("Log level set to ERROR")

    // Import implicits like $"columnName" after SparkSession creation
    import spark.implicits._

    // Step 2: Prepare the data_centers schema
    val dataCenterSchema = new StructType()
      .add("Facility_ID", StringType)
      .add("Facility_Name", StringType)
      .add("Operator", StringType)
      .add("City", StringType)
      .add("State", StringType)
      .add("Utility_Name", StringType)
      .add("Latitude", DoubleType)
      .add("Longitude", DoubleType)
      .add("Opening_Year", IntegerType)
      .add("Opening_Month", IntegerType)
      .add("Capacity_MW", DoubleType)
      .add("Verified", StringType)
      .add("Data_Source", StringType)
      .add("Source_URL", StringType)
      .add("Notes", StringType)

    val powerSchema = new StructType()
      .add("Utility_ID", StringType)
      .add("Utility_Name", StringType)
      .add("State", StringType)
      .add("Revenue_Thousands", DoubleType)
      .add("Sales_MWh", DoubleType)
      .add("Customers", DoubleType)
      .add("Year", IntegerType)
      .add("Price_Per_kWh", DoubleType)

    // Step 3: Load the data into DataFrames
    // Load the datacenters file
    val dataCenters = spark.read
      .option("header", "true")
      .schema(dataCenterSchema)
      .csv(args(0) + "/data_centers.csv")
    println(s"Loaded Data Centers: ${dataCenters.count()} rows")

    // Load the power costs into one dataframe
    val powerCosts = spark.read
      .option("header", "true")
      .schema(powerSchema)
      .csv(args(0) + "/cleaned_eia_years" + "/power_cost_*.csv")
    println(s"Loaded Power Costs: ${powerCosts.count()} rows")

    // Preview
    println("Preview of powerCosts:")
    powerCosts.show(10, truncate = false)

    // Step 4: Feed into the training of the model

    // Step N-1: Save output to disk

    // Step N: Stop the spark session
    spark.stop()

  }
}