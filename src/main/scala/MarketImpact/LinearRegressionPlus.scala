package MarketImpact

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

/**
  * Created by ceikit on 4/12/16.
  */
case class LinearRegressionPlus(tradesFile: String) {


  lazy val minuteClass =  MinuteAggregation(tradesFile)

  val dataSet =
    minuteClass.makeMinuteAggregateData()
      .filter( v => v.realizedVariance > 0.0)
      .persist()

  lazy val tradeFlowFeature: RDD[LabeledPoint] =
    dataSet.rdd
      .map(
        row => LabeledPoint(row.returns/math.sqrt(row.realizedVariance) ,
          Vectors.dense(row.tradeFlow/row.tradedVolume))
      )

}
