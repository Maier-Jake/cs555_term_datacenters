package DataCenterPrices

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.DecisionTreeRegressor

object Main {
  def main(args: Array[String]): Unit = {
    // Step 1: Create Spark session
    val spark = SparkSession.builder().appName("Impact of Data Centers on Residential Electricity Prices").getOrCreate()

    spark.sparkContext.setLogLevel("INFO")
    println("Log level set to INFO")

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

    val rawDatacenters = spark.read
      .option("header", "true")
      .schema(dataCenterSchema)
      .csv(args(0) + "/data_centers.csv")
      .na.fill(0.0, Seq("Capacity_MW")) // fill in the NULLs for now until Eric fills them in in the database
      .withColumn("Opening_Year", $"Opening_Year".cast("int"))
      .withColumn("Opening_Month", $"Opening_Month".cast("int"))
      .drop("Notes", "Source_URL", "Data_Source", "Verified",
        "Opening_Month", "Latitude", "Longitude")
    println(s"Loaded Data Centers: ${rawDatacenters.count()} rows")
    println("Preview of rawDatacenters:")
    rawDatacenters.show(10, truncate = false)

    // Step 3: Load the power_costs data
    val powerSchema = new StructType()
      .add("Utility_Name", StringType)
      .add("State", StringType)
      .add("Year", IntegerType)
      .add("Utility_ID", StringType)
      .add("Revenue_Thousands", DoubleType)
      .add("Sales_MWh", DoubleType)
      .add("Customers", DoubleType)
      .add("Price_Per_kWh", DoubleType)

    val powerCosts = spark.read
      .option("header", "true")
      .schema(powerSchema)
      .csv(args(0) + "/cleaned_eia_years" + "/power_cost_*.csv")
      .filter($"Customers" > 400 && $"Sales_MWh" > 400)
      .drop("Revenue_Thousands", "Sales_MWh")
      /* require that we have atleast a decent sized utility, 
       * I noticed that we had repeats because some EIA data splits utility companies per 
       * customer class but that split is not defined in the data */
      // Cleanup data, removing adjustment ids, converting strings to ints where appropriate
      .filter(!$"Utility_ID".isin("99999.0", "88888.0", "77777.0"))
      .withColumn("Utility_ID", col("Utility_ID").cast("double").cast("int"))
      .withColumn("Customers", col("Customers").cast("double").cast("int"))
      .cache()
    println(s"Loaded Power Costs: ${powerCosts.count()} rows")
    println("Preview of powerCosts:")
    powerCosts.show(10, truncate = false)

    // Step 4: Build panel of states x years

    val minYear = powerCosts.agg(min("Year")).as[Int].first()
    val maxYear = powerCosts.agg(max("Year")).as[Int].first()

    val years = spark.range(minYear, maxYear + 1).toDF("Year")
    // Yeah, there is probably an issue here if we get states that don't have any datacenters. Like idk I'm gonna guess mississippi and or louisiana don't have any lol
    val states = rawDatacenters.select("State").distinct()
    val stateYearGrid = states.crossJoin(years)

    // Step 5: aggregate data with the State x Year grid

    // Some of our datacenters are from BEFORE 2013, so lets add those in as the baseline for the dataset...
    val prePeriod = rawDatacenters
      .filter($"Opening_Year" < minYear)
      .groupBy("State")
      .agg(
        count("*").alias("Pre_DC"),
        sum("Capacity_MW").alias("Pre_MW")
      )

    val byYear = rawDatacenters
      .filter($"Opening_Year" >= minYear)
      .groupBy("State", "Opening_Year")
      .agg(
        count("*").alias("DC_Opened"),
        sum("Capacity_MW").alias("MW_Added")
      )
      .withColumnRenamed("Opening_Year", "Year")

    // println("Preview of byYear:")
    // byYear.show(20, truncate = false)

    val w = Window.partitionBy("State").orderBy("Year")
    val dcJoinedAndCum = stateYearGrid
      .join(byYear, Seq("State", "Year"), "left")
      .na.fill(0, Seq("DC_Opened", "MW_Added"))
      .join(prePeriod, Seq("State"), "left")
      .na.fill(0, Seq("Pre_DC", "Pre_MW"))
      .withColumn("Cum_DC", $"Pre_DC" + sum($"DC_Opened").over(w))
      .withColumn("Cum_MW", $"Pre_MW" + sum($"MW_Added").over(w))
      .drop("Pre_DC", "Pre_MW", "DC_Opened", "MW_Added")

    // Step 7: Add avg electricity prices

    val prices = powerCosts
      .groupBy("State", "Year")
      .agg(avg("Price_Per_kWh").alias("Avg_kWh"))

    val dcFull = dcJoinedAndCum
      .join(prices, Seq("State", "Year"), "left")
      .na.drop("any", Seq("Avg_kWh"))

    println("Preview of dcFull:")
    dcFull.show(20, truncate = false)

    val assembler = new VectorAssembler()
      .setInputCols(Array("Cum_DC", "Cum_MW"))
      .setOutputCol("features")

    val mlDF = assembler.transform(dcFull)
      .select("features", "Avg_kWh")
      .withColumnRenamed("Avg_kWh", "label")

    val mlSafeDF = mlDF.coalesce(4).cache().na.fill(0)
    val Array(train, test) = mlSafeDF.randomSplit(Array(0.8, 0.2), seed = 19)

    // Train the model
    println("Beginning training...")

    val dt = new DecisionTreeRegressor()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setMaxDepth(5)
      .setMinInstancesPerNode(5)

    val model = dt.fit(train)
    println("Training completed.")

    println(s"Max Depth: ${model.getMaxDepth}")
    println(s"Num Nodes: ${model.numNodes}")

    // Save the model to hdfs
    val modelOutputPath = "hdfs:///results/model"
    model.write.overwrite().save(modelOutputPath)
    println(s"Saved model to $modelOutputPath")
    // val loaded = DecisionTreeRegressionModel.load("hdfs:///datacenter_model_output/dt_model")
    
    // Evaluate on test set
    val predictions = model.transform(test)

    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")

    println("RMSE: " + evaluator.setMetricName("rmse").evaluate(predictions))
    println("R2:   " + evaluator.setMetricName("r2").evaluate(predictions))

    // Collect all log messages to write to hdfs
    val results = Seq(
      s"Max Depth: ${model.getMaxDepth}",
      s"Num Nodes: ${model.numNodes}",
      s"RMSE: ${evaluator.setMetricName("rmse").evaluate(predictions)}",
      s"R2:   ${evaluator.setMetricName("r2").evaluate(predictions)}"
    )
    // Convert to DF and save to HDFS
    spark.createDataset(results).coalesce(1)
      .write.mode("overwrite")
      .text("hdfs:///datacenter_model_output/run_logs")

    spark.stop()

  }
}