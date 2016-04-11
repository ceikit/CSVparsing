import Checks.DataCheckSingleAsset

object TradingSession {

  def main(args: Array[String]): Unit = {

    val tradesFile = "sashastoikov@gmail.com_5020.T_tradesQuotes_20130103_20150909.csv"
    val quoteFile = "sashastoikov@gmail.com_5020.T_events_20130101_20131206_frequency_-1.csv.gz"

    lazy val dataCheck = DataCheckSingleAsset(tradesFile, quoteFile)

    dataCheck.tradingSessionCheck.summaryTradesOff.printSummary()
    dataCheck.tradingSessionCheck.summaryQuotesOff.printSummary()


  }
}
