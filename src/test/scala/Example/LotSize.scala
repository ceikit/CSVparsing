
object LotSize {

  def main(args: Array[String]): Unit = {

    val tradesFile = "sashastoikov@gmail.com_1802.T_tradesQuotes_20130103_20150909.csv.gz"

    val quoteFile = "sashastoikov@gmail.com_1802.T_events_20130103_20131206_frequency_-1.csv.gz"

    lazy val dataCheck = DataCheckSingleAsset(tradesFile, quoteFile)

    println(dataCheck.sizeCheck.minTradeSize, dataCheck.sizeCheck.averageTradeSize,dataCheck.sizeCheck.minQuoteSize, dataCheck.sizeCheck.averageQuoteSize)

    println(dataCheck.sizeCheck.minTradeSize , dataCheck.sizeCheck.minQuoteSize, dataCheck.sizeCheck.median, dataCheck.sizeCheck.roundMultiples)
    dataCheck.dataPlot.tradeDataPlot.sizeHistogram(100)

  }
}
