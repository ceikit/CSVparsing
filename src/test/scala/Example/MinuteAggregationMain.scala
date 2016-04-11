import MarketImpact.{MinuteAggregation, NormalizedIntradayMinuteData, DailyAggregation, NormalizedIntradayData}
import org.apache.spark.sql.Dataset


object MinuteAggregationMain {

  def main(args: Array[String]): Unit = {

    val tradesFile = "sashastoikov@gmail.com_8604.T_tradesQuotes_20130103_20150909.csv.gz"

    lazy val minuteClass =  MinuteAggregation(tradesFile)
    lazy val dailyClass = DailyAggregation(minuteClass)

    val prova: Dataset[NormalizedIntradayMinuteData] =
      NormalizedIntradayData(dailyClass).makeNormalizedIntradayDataSet().persist()

//;
//    val tradeFlow = prova.rdd.map(_.tradeFlow).histogram(100)
//    val tradeFlowNormalized =
//      prova.rdd.filter(_.tradedVolume > 0.0).map(t => t.tradeFlow/t.tradedVolume).histogram(100)
//
//    val normalizedReturns = prova.rdd.filter(_.realizedVariance > 0.0)
//      .map(t=> t.returns/ math.sqrt(t.realizedVariance)).histogram(100)
//
//    val returns = prova.rdd.map(_.returns * 100).histogram(100)
//
//
//    histogram(tradeFlow._1.zip( tradeFlow._2).toList)
//     legend(List("Trade Flow"))
//
//    histogram(tradeFlowNormalized._1.zip(tradeFlowNormalized._2).toList )
//    legend(List("Trade Flow divided by Volume"))
//
//    histogram(returns._1.zip(returns._2).toList)
//   legend(List("Checks.Returns"))
//
//    histogram(normalizedReturns._1.zip(normalizedReturns._2).toList)
//    legend(List("Normalized return w.r.t. realized Volatility"))

    prova.show()
  }
}
//  Plot histograms of following variables across all stocks and time periods
//   1‐minute trade flow
//   1‐minute trade flow divided by traded volume (drop zero‐volume observations)
//   1‐minute return
//   1‐minute return divided by square root of realized variance (drop zero‐variance
//  observations)
//   Daily average depth
//   Daily average spread
//   Daily volatility
//   Daily traded volume