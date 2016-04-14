package MarketImpact

import breeze.linalg.max
import org.apache.spark.rdd.RDD

/**
  * Created by ceikit on 4/8/16.
  */
case class DailyAggregation(minuteClass : MinuteAggregation){

  import minuteClass.hive.implicits._

  lazy val minuteData: RDD[AggregateMinuteData] = minuteClass.makeMinuteAggregateData().rdd

  lazy val dailyTradedVolume =
    minuteData.map( k => k.dateString -> k.tradedVolume).foldByKey(0.0)(_ + _)
      .toDF("dateString", "dailyTradedVolume")

  lazy val dailyVolatility =
    minuteData.map( k => k.dateString -> k.realizedVariance).foldByKey(0.0)(_ + _).mapValues(math.sqrt)
      .toDF("dateStringV", "dailyVolatility")

  lazy val dailyAverageSpread =
    minuteData
      .map( k => k.dateString -> (k.averageSpread,1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues{case(d, i) => d/i.toFloat}
      .toDF("dateStringS", "dailyAverageSpread")


  def makeDailyAggregateData(): RDD[AggregateDailyData] = {
    dailyTradedVolume
      .join(dailyVolatility, dailyTradedVolume("dateString") === dailyVolatility("dateStringV"))
      .join(dailyAverageSpread, dailyTradedVolume("dateString") === dailyAverageSpread("dateStringS"))
      .select("dateString", "dailyTradedVolume", "dailyVolatility", "dailyAverageSpread")
      .map( r =>
      AggregateDailyData(
        r(0).toString,
        r(1).asInstanceOf[Double],
        r(2).asInstanceOf[Double],
        r(3).asInstanceOf[Double])
      ).toDS().rdd
  }
}

case class NumericDailyAggregation(minuteClass : NumericMinuteAggregation){


  lazy val minuteData: RDD[AggregateNumericalMinuteData] = minuteClass.makeMinuteAggregateData()

  def makeDailyAggregateData(): RDD[AggregateNumericDailyData] = {

    def combOp(d: AggregateNumericDailyAux, m: AggregateNumericalMinuteAux) : AggregateNumericDailyAux = {
      AggregateNumericDailyAux(
        m.day,
        m.tradedVolume + d.dailyTradedVolume,
        m.realizedVariance + d.dailyVolatility,
        m.averageSpread + d.dailyAverageSpread,
        d.cont + 1)
    }

    def seqOp(d1: AggregateNumericDailyAux, d2: AggregateNumericDailyAux) : AggregateNumericDailyAux = {
      AggregateNumericDailyAux(
        max(d1.day, d2.day),
        d1.dailyTradedVolume + d2.dailyTradedVolume,
        d1.dailyVolatility + d2.dailyVolatility,
        d1.dailyAverageSpread + d2.dailyAverageSpread,
        d1.cont + d2.cont)
    }


    minuteData.map( k => k.day -> k)
      .mapValues(v => AggregateNumericalMinuteAux(v.day,v.tradedVolume, v.averageSpread, v.realizedVariance))
      .aggregateByKey(AggregateNumericDailyAux(0,0,0,0,0))(combOp, seqOp )
      .mapValues(v =>
        AggregateNumericDailyData(
          v.day,
          v.dailyTradedVolume,
          math.sqrt(v.dailyVolatility),v.dailyAverageSpread/v.cont)).values

  }



}
