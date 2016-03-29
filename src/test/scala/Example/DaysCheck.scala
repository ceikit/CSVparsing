
object DaysCheck {

  def main(args: Array[String]): Unit ={

    val tradesFile = "sashastoikov@gmail.com_5020.T_tradesQuotes_20130103_20150909.csv"

    val quoteFile = "sashastoikov@gmail.com_5020.T_events_20130101_20131206_frequency_-1.csv.gz"

    lazy val dataCheck = DataCheckSingleAsset(tradesFile, quoteFile)

    println("THE DAYS MATCH: " + dataCheck.daysMatch)
    println("the ASK > BID always: " + dataCheck.quoteCheck.bidLessThanAsk)

    dataCheck.dataPlot.tradeDataPlot.tradesPerDayPlot
    dataCheck.dataPlot.tradeDataPlot.volumePerdayPlot


    println("the FIRST Trade day is: " + dataCheck.tradeCheck.firstDay)
    println("the LAST Trade day is: " + dataCheck.tradeCheck.lastDay)
    println("the FIRST Quote day is: " + dataCheck.quoteCheck.firstDay)
    println("the LAST Quote day is: " + dataCheck.quoteCheck.lastDay)




  }

}
