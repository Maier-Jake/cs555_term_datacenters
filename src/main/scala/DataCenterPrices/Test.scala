package DataCenterPrices

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder, VectorAssembler}
import org.apache.spark.ml.feature.StringIndexerModel
import org.apache.spark.ml.feature.OneHotEncoderModel
import org.apache.spark.ml.regression.RandomForestRegressor
import org.apache.spark.ml.regression.RandomForestRegressionModel


object Test {
  def main(args: Array[String]): Unit = {
    // Step 1: Create Spark session
    val spark = SparkSession.builder().appName("Impact of Data Centers on Residential Electricity Prices - Test").getOrCreate()

    spark.sparkContext.setLogLevel("INFO")
    println("Log level set to INFO")

    import spark.implicits._

    // Load the saved model from the other file
    val model = RandomForestRegressionModel.load("hdfs:///results/model")

    // Load the saved dataset too
    val dc = spark.read.parquet("hdfs:///results/dcWithPrev")
    dc.printSchema()

    // Rebuild the one-hot encoding for states
    val indexerModel = StringIndexerModel.load("hdfs:///results/stateIndexer")
    val encoderModel = OneHotEncoderModel.load("hdfs:///results/stateEncoder")

    val indexed = indexerModel.transform(dc)
    val encoded = encoderModel.transform(indexed)

    // Step 11: Assemble the model
    val assembler = new VectorAssembler()
      .setInputCols(Array("DC_Opened", "PrevPrice", "StateVec"))
      .setOutputCol("features")

    val withFeatures = assembler.transform(encoded)

    // Get my baseline numbers with the true numbers
    val baseline = model.transform(withFeatures)
      .select(
        $"State",
        $"Year",
        $"DC_Opened",
        $"PrevPrice",
        $"Avg_kWh",
        $"prediction".alias("Pred_baseline")
      )

    def runScenario(df: DataFrame, factor: Double): DataFrame = { 
      // apply my factor to the real data
      val modified = df.withColumn("DC_Opened", col("DC_Opened") * factor)
      val withFeatures = assembler.transform(modified)

      model.transform(withFeatures)
        .select($"State", $"Year", $"DC_Opened", $"prediction")
    }

    def renameScenario(df: DataFrame, name: String): DataFrame = {
      df.withColumnRenamed("DC_Opened", s"DC_$name")
        .withColumnRenamed("prediction", s"Pred_$name")
    }

    // run all the made up scenarios with scaled up/down data centers
    val scenarios = Seq(
      ("minus10", 0.90),
      ("plus10", 1.10),
      ("plus20", 1.20),
      ("slow75", 0.75),
      ("fast150", 1.50),
      ("ban0", 0.0)
    )

    val scenarioDfs = scenarios.map { case (name, factor) =>
      renameScenario(runScenario(encoded, factor), name)
    }

    // I did this so I don't repeat my PrevPrice over and over in the 'all' output
    val all = scenarioDfs.foldLeft(baseline) { (df1, df2) =>
      df1.join(df2, Seq("State", "Year"), "left")
    }

    all.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .csv("hdfs:///results/counterfactuals")

    println("Counterfactual scenarios written to: hdfs:///results/counterfactuals")

    spark.stop()

  }
}