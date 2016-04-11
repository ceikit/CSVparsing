import Checks.DataCheckSingleAsset

object SanityCheck {

  def main(args: Array[String]): Unit ={

    val tradesFile = "sashastoikov@gmail.com_5020.T_tradesQuotes_20130103_20150909.csv"
    val quoteFile = "sashastoikov@gmail.com_5020.T_events_20130101_20131206_frequency_-1.csv.gz"

    lazy val dataCheck = DataCheckSingleAsset(tradesFile, quoteFile)

    val tradesSanityCheck = dataCheck.tradeCheck.sanityChecksTrades

    println("number of trades: " + dataCheck.tradesData.count())
    println("the number of (suspicious) trades at price 0 is: " + tradesSanityCheck.numberOfZeroPrice)
    println("the number of (suspicious) trades with SIZE 0 is: " + tradesSanityCheck.numberOfTradesWithSizeZero)
    println("the total number of days in the dataset is: " + tradesSanityCheck.totalNumberOfDays)

    println("the total number of days with ZERO trades is: " + tradesSanityCheck.daysWithZeroTrades.count())
    println("number of zero spread quote: " + dataCheck.quoteCheck.sanityChecksQuotes.numberOfZeroSPREAD)


  }

}
