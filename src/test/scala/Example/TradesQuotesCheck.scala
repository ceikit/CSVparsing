
object TradesQuotesCheck {

  def main(args: Array[String]): Unit = {

    val tradesFile = "sashastoikov@gmail.com_5020.T_tradesQuotes_20130103_20150909.csv"
    val quoteFile = "sashastoikov@gmail.com_5020.T_events_20130101_20131206_frequency_-1.csv.gz"

    lazy val dataCheck = DataCheckSingleAsset(tradesFile, quoteFile)

    println("average number of trades per day: " + dataCheck.tradeCheck.tradesPerDay.values.map(p=> 5*3600/p.toFloat).mean())

    println("total number of Trades: " + dataCheck.tradeCheck.tradesData.count())
    println("total number of Quotes: " + dataCheck.quoteCheck.quoteData.count())

    dataCheck.dataPlot.tradeDataPlot.tradesToQuoteRatioDailyPlot


  }

}
