package DataCenterPrices

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, DoubleType, StringType, IntegerType}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.GBTRegressor
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

    // Step 2: Load the data_centers data
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
    val dataCenters = spark.read
      .option("header", "true")
      .schema(dataCenterSchema)
      .csv(args(0) + "/data_centers.csv")
      .drop("Notes", "Source_URL", "Data_Source", "Verified",
        "Opening_Month", "Latitude", "Longitude")

    println(s"Loaded Data Centers: ${dataCenters.count()} rows")

    println("Preview of dataCenters:")
    dataCenters.show(10, truncate = false)

    // Step 3: Load the power_costs data
    val powerSchema = new StructType()
      .add("Utility_ID", StringType)
      .add("Utility_Name", StringType)
      .add("State", StringType)
      .add("Revenue_Thousands", DoubleType)
      .add("Sales_MWh", DoubleType)
      .add("Customers", DoubleType)
      .add("Year", IntegerType)
      .add("Price_Per_kWh", DoubleType)
    val powerCosts = spark.read
      .option("header", "true")
      .schema(powerSchema)
      .csv(args(0) + "/cleaned_eia_years" + "/power_cost_*.csv")
      .drop("Revenue_Thousands","Sales_MWh")
      .filter($"Customers" > 400 && $"Sales_MWh" > 400)
      /* require that we have atleast a decent sized utility, 
       * I noticed that we had repeats because some EIA data splits utility companies per 
       * customer class but that split is not defined in the data */
      // Cleanup data, removing adjustment ids, converting strings to ints where appropriate
      .filter(!$"Utility_ID".isin("99999.0", "88888.0", "77777.0"))
      .withColumn(
        "Utility_ID",
        regexp_replace($"Utility_ID", "\\.0$", "").cast(IntegerType)
      )
      .withColumn(
        "Customers",
        regexp_replace($"Customers", "\\.0$", "").cast(IntegerType)
      )
    println(s"Loaded Power Costs: ${powerCosts.count()} rows")

    println("Preview of powerCosts:")
    powerCosts.show(10, truncate = false)

    // Step 4: Feed into the training of the model
    // Model Goal: Determine if the opening of data centers is causing residential energy prices to increase.
    // We need te able to feed "theoretical" data center openings to see how the state might raise their residential energy prices.



    spark.stop()

  }
}