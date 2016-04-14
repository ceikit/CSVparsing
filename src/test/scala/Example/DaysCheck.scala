package Example
import Checks.DataCheckSingleAsset

object DaysCheck {

  def main(args: Array[String]): Unit ={

    val tradesFile = "sashastoikov@gmail.com_7203.T_tradesQuotes_20130103_20150909.csv.gz"
    val quoteFile = "sashastoikov@gmail.com_7203.T_events_20130103_20130706_frequency_-1.csv.gz"

    lazy val dataCheck = DataCheckSingleAsset(tradesFile, quoteFile)

    println("THE DAYS MATCH: " + dataCheck.daysMatch + '\n' +
      "the ASK > BID always: " + dataCheck.quoteCheck.bidLessThanAsk + '\n' +
      "the FIRST Trade day is: " + dataCheck.tradeCheck.firstDay + '\n' +
      "the LAST Trade day is: " + dataCheck.tradeCheck.lastDay + '\n' +
      "the FIRST Quote day is: " + dataCheck.quoteCheck.firstDay + '\n' +
      "the LAST Quote day is: " + dataCheck.quoteCheck.lastDay )

    dataCheck.dataPlot.tradeDataPlot.tradesPerDayPlot
    dataCheck.dataPlot.tradeDataPlot.volumePerdayPlot



  }

}
