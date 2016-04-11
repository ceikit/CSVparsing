package MarketImpact

/**
  * Created by ceikit on 4/8/16.
  */
case class DailyAggregation(minuteClass : MinuteAggregation){

  import minuteClass.hive.implicits._

  lazy val minuteData = minuteClass.makeMinuteAggregateData().rdd

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


  def makeDailyAggregateData() = {
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
