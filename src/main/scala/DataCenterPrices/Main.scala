package DataCenterPrices

import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructType, StructField, DoubleType, StringType, IntegerType}
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
      .add("Opening_Year", DoubleType)
      .add("Opening_Month", DoubleType)
      .add("Capacity_MW", DoubleType)
      .add("Verified", StringType)
      .add("Data_Source", StringType)
      .add("Source_URL", StringType)
      .add("Notes", StringType)
    val dataCenters = spark.read
      .option("header", "true")
      .schema(dataCenterSchema)
      .csv(args(0) + "/data_centers.csv")
      .na.fill(0.0, Seq("Capacity_MW")) // fill in the NULLs for now until Eric fills them in in the database
      .withColumn("Opening_Year", $"Opening_Year".cast("int"))
      .withColumn("Opening_Month", $"Opening_Month".cast("int"))
      .drop("Notes", "Source_URL", "Data_Source", "Verified",
        "Opening_Month", "Latitude", "Longitude")
    // println(s"Loaded Data Centers: ${dataCenters.count()} rows")

    // println("Preview of dataCenters:")
    // dataCenters.show(10, truncate = false)

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
      .withColumn("Utility_ID", col("Utility_ID").cast("double").cast("int"))
      .withColumn("Customers", col("Customers").cast("double").cast("int"))
      .cache()
    // println(s"Loaded Power Costs: ${powerCosts.count()} rows")

    // println("Preview of powerCosts:")
    // powerCosts.show(10, truncate = false)

    // Step 4: Feed into the training of the model
    // Model Goal: Determine if the opening of data centers is causing residential energy prices to increase.
    // We need the able to feed "theoretical" data center openings to see how the state might raise their residential energy prices.

    val minYear = powerCosts.agg(min("Year")).as[Int].first()
    val maxYear = powerCosts.agg(max("Year")).as[Int].first()
    println(s"Year range: $minYear to $maxYear")
    val years = spark.range(minYear, maxYear + 1).withColumnRenamed("id", "Year")
    // Yeah, there is probably an issue here if we get states that don't have any datacenters. Like idk I'm gonna guess mississippi and or louisiana don't have any lol
    val states = dataCenters.select("State").distinct()
    val stateYearGrid = states.crossJoin(years)

    // states.show(52)

    // Get a dataframe with the datacenters grouped by the state they are in and the year they are opened.
    val dcByStateYear = dataCenters
      .groupBy($"State", $"Opening_Year")
      .agg(
        count("*").alias("Num_DataCenters_Opened"),
        sum($"Capacity_MW").alias("Total_Capacity_Added")
      )
      .withColumnRenamed("Opening_Year", "Year")
    
    // println("Preview of dcByStateYear:")
    // dcByStateYear.show(20, truncate = false)

    // Now I need to join my overall stateYearGrid with the datacenter data
    val w = Window.partitionBy("State").orderBy("Year")

    // Some of our datacenters are from BEFORE 2013, so lets add those in as the baseline for the dataset...
    val prePeriodDC = dataCenters
      .filter($"Opening_Year" < minYear)
      .groupBy("State")
      .agg(
        count("*").alias("Preexisting_DataCenters"),
        sum($"Capacity_MW").alias("Preexisting_Capacity_MW")
      )

    val dcFull = stateYearGrid
      .join(dcByStateYear, Seq("State", "Year"), "left")
      .na.fill(0, Seq("Num_DataCenters_Opened", "Total_Capacity_Added"))
      // add the baseline number of datacenters
      .join(prePeriodDC, Seq("State"), "left")
      .na.fill(0, Seq("Preexisting_DataCenters", "Preexisting_Capacity_MW"))
      .withColumn("Cumulative_DataCenters",
        $"Preexisting_DataCenters" + sum($"Num_DataCenters_Opened").over(w)
      )
      .withColumn("Cumulative_Capacity_MW",
        $"Preexisting_Capacity_MW" + sum($"Total_Capacity_Added").over(w)
      )
      .drop("Num_DataCenters_Opened", "Total_Capacity_Added", "Preexisting_DataCenters", "Preexisting_Capacity_MW")

    // println("Preview of dcFull:")
    // dcFull.filter($"State" === "VA")
    //   .orderBy("Year")
    //   .show(20, truncate = false)

    // Now we need to get the average electricity price for each state for each year and add that as columns to dcFull
    val stateYearPrices = powerCosts
      .groupBy("State", "Year")
      .agg(
        avg("Price_Per_kWh").alias("Avg_Residential_kWh")
      )

    val fullPanel = dcFull
      .join(stateYearPrices, Seq("State", "Year"), "left")
      .na.drop("any", Seq("Avg_Residential_kWh"))

    // Any data center that opened before minYear in EIA is just part of the starting conditions of the grid.
    // We will not estimate its opening shock... we will just treat it as baseline load.

    fullPanel.show()
    spark.stop()

  }
}