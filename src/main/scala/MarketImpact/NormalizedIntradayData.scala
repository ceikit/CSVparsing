package MarketImpact

import org.apache.spark.sql.{DataFrame, UserDefinedFunction}

/**
  * Created by ceikit on 4/8/16.
  */
case class NormalizedIntradayData(dailyAggregate: DailyAggregation){

  val hive = dailyAggregate.minuteClass.hive

  import dailyAggregate.minuteClass.hive.implicits._

  val dailyAggregateData: DataFrame = dailyAggregate.makeDailyAggregateData().toDF()
  val minuteAggregateData: DataFrame = dailyAggregate.minuteData.toDF()

  def makeNormalizedIntradayDataSet () = {

    //Traded volume percentage (1‐minute traded volume / daily volume)
    // Normalized volatility (square‐root of realized variance / daily volatility)
    // Normalized depth (average depth / daily average depth)
    // Normalized spread (average spread / daily average spread)


    val volumePercentage: UserDefinedFunction =
      hive.udf.register("volumePercentage", (x: Double, y: Double) => x/y)

    val normalizedSpread: UserDefinedFunction =
      hive.udf.register("normalizedSpread", (x: Double, y: Double) => x/y)

    val normalizedVolatility =
      hive.udf.register("normalizedVolatility", (x: Double, y: Double) => math.sqrt(x)/y)



    val aggregate = dailyAggregateData.join(minuteAggregateData, "dateString")
      .groupBy("dateString", "hour", "tradedVolume", "tradeFlow", "averageSpread", "realizedVariance", "returns",
        "dailyTradedVolume", "dailyVolatility", "dailyAverageSpread")
      .agg(
        volumePercentage(minuteAggregateData("tradedVolume"),dailyAggregateData("dailyTradedVolume")),
        normalizedVolatility(minuteAggregateData("realizedVariance"), dailyAggregateData("dailyVolatility")),
        normalizedSpread(minuteAggregateData("averageSpread"), dailyAggregateData("dailyAverageSpread"))
      )
      .withColumnRenamed("UDF(tradedVolume,dailyTradedVolume)", "volumePercentage")
      .withColumnRenamed("UDF(realizedVariance,dailyVolatility)", "normalizedVolatility")
      .withColumnRenamed("UDF(averageSpread,dailyAverageSpread)", "normalizedSpread")
      .select("dateString", "hour", "tradedVolume", "tradeFlow", "averageSpread", "realizedVariance", "returns",
        "volumePercentage", "normalizedVolatility", "normalizedSpread")
      .map(r =>
      NormalizedIntradayMinuteData(
        r(0).toString,
        r(1).toString,
        r(2).asInstanceOf[Double],
        r(3).asInstanceOf[Double],
        r(4).asInstanceOf[Double],
        r(5).asInstanceOf[Double],
        r(6).asInstanceOf[Double],
        r(7).asInstanceOf[Double],
        r(8).asInstanceOf[Double],
        r(9).asInstanceOf[Double])
      )

    aggregate.toDF().coalesce(1).write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("minuteAggregate.csv")

//    aggregate.map { c =>
//        val date = c.dateString
//        val hour = c.hour
//        val tradedVolume = c.tradedVolume
//        val tradeFlow = c.tradeFlow
//        val averageSpread = c.averageSpread
//        val realizedVariance = c.realizedVariance
//        val returns = c.returns
//        val volumePercentage = c.volumePercentage
//        val normalizedVolatility = c.normalizedVolatility
//        val normalizedSpread = c.normalizedSpread
//        s"$date, $hour, $tradedVolume, $tradeFlow, $averageSpread, $realizedVariance, $returns, $volumePercentage, $normalizedVolatility, $normalizedSpread"
//      }.toDF().coalesce(1)
//        .write.format("csv").save(dailyAggregate.minuteClass.tradesFile + "_MinuteAggregate")

    aggregate.toDS()

  }
}
