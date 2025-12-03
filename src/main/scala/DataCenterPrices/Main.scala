package DataCenterPrices

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder, VectorAssembler}
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
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
    // println(s"Loaded Data Centers: ${rawDatacenters.count()} rows")
    // println("Preview of rawDatacenters:")
    // rawDatacenters.show(10, truncate = false)

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
      /* require that we have atleast a decent sized utility, 
       * I noticed that we had repeats because some EIA data splits utility companies per 
       * customer class but that split is not defined in the data */
      // Cleanup data, removing adjustment ids, converting strings to ints where appropriate
      .filter(!$"Utility_ID".isin("99999.0", "88888.0", "77777.0"))
      .withColumn("Utility_ID", col("Utility_ID").cast("double").cast("int"))
      .withColumn("Customers", col("Customers").cast("double").cast("int"))
      .drop("Revenue_Thousands", "Sales_MWh")
      .cache()
    // println(s"Loaded Power Costs: ${powerCosts.count()} rows")
    // println("Preview of powerCosts:")
    // powerCosts.show(10, truncate = false)

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
      )

    // datacenters DURING the period I've got price data...
    val byYear = rawDatacenters
      .filter($"Opening_Year" >= minYear)
      .groupBy("State", "Opening_Year")
      .agg(
        count("*").alias("DC_Opened"),
      )
      .withColumnRenamed("Opening_Year", "Year")

    println("Preview of byYear:")
    byYear.show(20, truncate = false)

    val w = Window.partitionBy("State").orderBy("Year")
    val cumulativeDatacenters = stateYearGrid
      .join(byYear, Seq("State", "Year"), "left")
      .na.fill(0, Seq("DC_Opened"))
      .join(prePeriod, Seq("State"), "left")
      .na.fill(0, Seq("Pre_DC"))
      .withColumn("Cumulative_DC", $"Pre_DC" + sum($"DC_Opened").over(w))
      .drop("Pre_DC")

    // Step 7: Add avg electricity prices

    val prices = powerCosts
      .groupBy("State", "Year")
      .agg(avg("Price_Per_kWh").alias("Avg_kWh"))

    // avg prices now added to my datacenters
    val dcFull = cumulativeDatacenters
      .join(prices, Seq("State", "Year"), "left")
      .na.drop("any", Seq("Avg_kWh"))

    // Step 8: One-hot encode the states

    // Some states are more expensive than others, don't let that sway the model...
    val stateIndexer = new StringIndexer()
      .setInputCol("State")
      .setOutputCol("StateIndex")
      .setHandleInvalid("keep")
    val stateEncoder = new OneHotEncoder()
      .setInputCol("StateIndex")
      .setOutputCol("StateVec")

    // Step 9: Add the previous price as a data point for each row...

    val dcWithPrev = dcFull
      .withColumn("PrevPrice", lag("Avg_kWh", 1).over(w))
      .na.drop("any", Seq("PrevPrice"))

    // Step 10: Reduce noise

    // If a datacenter hasn't opened in 3 years, drop the rows. Helps reduce noise in the model...
    val threeYearWindow = Window
      .partitionBy("State")
      .orderBy("Year")
      .rowsBetween(-3, 1)
    val dcFiltered = dcWithPrev
      .withColumn("RecentOpenings", sum($"DC_Opened").over(threeYearWindow))
      .filter($"RecentOpenings" > 0) // keep only years where at least 1 DC opened in last 3 years
      .drop("RecentOpenings")
    // gotta save this to use in the test scenarios file
    dcFiltered.write.mode("overwrite").parquet("hdfs:///results/dcWithPrev")
    // println("Preview of dcWithPrev:")
    // dcWithPrev.show(40, truncate = false)

    // Step 11: Assemble the model

    val assembler = new VectorAssembler()
      .setInputCols(Array("DC_Opened", "PrevPrice", "StateVec"))
      .setOutputCol("features")

    val indexed = stateIndexer.fit(dcFiltered).transform(dcFiltered)
    val encoded = stateEncoder.fit(indexed).transform(indexed)

    // gotta save this to use in the test scenarios file
    stateIndexer.fit(dcFiltered).write.overwrite().save("hdfs:///results/stateIndexer")
    stateEncoder.fit(indexed).write.overwrite().save("hdfs:///results/stateEncoder")

    println("Preview of encoded:")
    encoded.show(40, truncate = false)

    // dataframe for the model
    val dataframe = assembler.transform(encoded)
      .select("features", "Avg_kWh")
      .withColumnRenamed("Avg_kWh", "label")

    val Array(train, test) = dataframe.randomSplit(Array(0.8, 0.2), seed = 19)

    // Step 12: Train the model
    println("Beginning training...")

    val rf = new RandomForestRegressor()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setNumTrees(200)
      .setMaxDepth(8)
      .setMinInstancesPerNode(5)
      .setSubsamplingRate(1.0)

    // Hyper-parameter tuning, this is fine because we have a tiny dataset.
    val paramGrid = new ParamGridBuilder()
      .addGrid(rf.numTrees, Array(100, 200, 300))
      .addGrid(rf.maxDepth, Array(4, 6, 8))
      .build()

    val evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse")

    // For the hyper-parameter tuning
    val cv = new CrossValidator()
      .setEstimator(rf)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(4)

    val cvModel = cv.fit(train)

    println("Training completed.")

    // Step 13: Save the model for use in the test file

    // Get the best model
    val model = cvModel.bestModel.asInstanceOf[org.apache.spark.ml.regression.RandomForestRegressionModel]

    // Save the model to hdfs
    val modelOutputPath = "hdfs:///results/model"
    model.write.overwrite().save(modelOutputPath)
    println(s"Saved model to $modelOutputPath")
    // val loaded = DecisionTreeRegressionModel.load("hdfs:///results/model")

    // Evaluate on test set
    val predictions = model.transform(test)

    // Collect all log messages to write to hdfs
    val results = Seq(
      s"RMSE: ${evaluator.setMetricName("rmse").evaluate(predictions)}",
      s"R2:   ${evaluator.setMetricName("r2").evaluate(predictions)}"
    )
    spark.createDataset(results).coalesce(1)
      .write.mode("overwrite")
      .text("hdfs:///results/run_logs")
    println("Saved run logs to hdfs:///results/run_logs")

    spark.stop()

  }
}