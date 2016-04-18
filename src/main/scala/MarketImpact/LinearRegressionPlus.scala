package MarketImpact

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SQLContext}

/**
  * Created by ceikit on 4/12/16.
  */
case class LinearRegressionPlus(minuteAggregateFile: String) {

  val sqlContext = new SQLContext( ParsingStructure.SparkCSVParsing.sc)

  import ParsingStructure.SparkCSVParsing.hiveContext.implicits._

  lazy val minuteAggregateData: Dataset[AggregateNumericalMinuteData] = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
    .option("inferSchema", "true") // Automatically infer data types
    .load(minuteAggregateFile).as[AggregateNumericalMinuteData]
    .filter( v => v.realizedVariance > 0.0)
    .persist()

  lazy val tradeFlowFeature: RDD[LabeledPoint] =
    minuteAggregateData.rdd
      .map(
        row => LabeledPoint(row.returns/math.sqrt(row.realizedVariance) ,
          Vectors.dense(row.tradeFlow/row.tradedVolume))
      )

}
