package MarketImpact

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ceikit on 4/12/16.
  */
case class LinearRegression(fileName: String) {

  val conf = new SparkConf().setMaster("local[*]").setAppName("linear regression").set("spark.executor.memory", "4g").set("spark.driver.memory", "8g")
  val sc = new SparkContext(conf)
  val hiveCtx = new HiveContext(sc)
  import hiveCtx.implicits._


  lazy val minuteDataChinese = hiveCtx.read
    .format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
    .option("inferSchema", "true") // Automatically infer data types
    .load(fileName)
    .select("TradeDate","TradeTime", "TradeVolume","TradeFlow", "AverageSpread" ,"RealizedVariance", "Return")
    .withColumnRenamed("TradeDate", "dateString")
    .withColumnRenamed("TradeTime", "hour")
    .withColumnRenamed("TradeVolume", "tradedVolume")
    .withColumnRenamed("TradeFlow", "tradeFlow")
    .withColumnRenamed("AverageSpread", "averageSpread")
    .withColumnRenamed("RealizedVariance", "realizedVariance")
    .withColumnRenamed("Return", "returns")
    .as[NumericAggregateMinuteData]
    .filter( v => v.realizedVariance != 0.0)
    .persist()

  lazy val minuteData: Dataset[NormalizedIntradayMinuteData] = hiveCtx.read
    .format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
    .option("inferSchema", "true") // Automatically infer data types
    .load(fileName)
    .as[NormalizedIntradayMinuteData]
    .filter( v => v.realizedVariance > 0.0)
    .persist()

  lazy val tradeFlowFeatureChinese: RDD[LabeledPoint] =
    minuteDataChinese.rdd
      .map(
        row => LabeledPoint(row.returns/math.sqrt(row.realizedVariance) ,
        Vectors.dense(row.tradeFlow/row.tradedVolume))
      )

  lazy val tradeFlowFeature: RDD[LabeledPoint] =
    minuteData.rdd
      .map(
        row => LabeledPoint(row.returns/math.sqrt(row.realizedVariance) ,
          Vectors.dense(row.tradeFlow/row.tradedVolume))
      )




}
