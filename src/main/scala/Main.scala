


object Main {

  def main(args: Array[String]): Unit ={

    val tradesFile = "sashastoikov@gmail.com_5020.T_tradesQuotes_20130103_20150909.csv"

    val quoteFile = "sashastoikov@gmail.com_5020.T_events_20130101_20131206_frequency_-1.csv.gz"

      /*
    val quoteData: List[Quote] = SparkCSVParsing.makeQuotesArray(quotesFile)
      .filter(q => q._2.bid > 0 && q._2.ask >0).values.take(10000).toList

    line(quoteData.map(_.ask))
    hold
    line(quoteData.map(_.bid)) */


    val dataCheck = DataCheckSingleAsset(tradesFile, quoteFile)

    //println("THE DAYS MATCH: " + dataCheck.daysMatch)
    //println("the ASK > BID always: " + dataCheck.quoteCheck.bidLessThanAsk)

    //dataCheck.dataPlot.tradeDataPlot.tradesPerDayPlot
    //dataCheck.dataPlot.tradeDataPlot.volumePerdayPlot

    /*
    println("the FIRST Trade day is: " + dataCheck.tradeCheck.firstDay)
    println("the LAST Trade day is: " + dataCheck.tradeCheck.lastDay)
    println("the FIRST Quote day is: " + dataCheck.quoteCheck.firstDay)
    println("the LAST Quote day is: " + dataCheck.quoteCheck.lastDay)
    */

    lazy val totTrades = dataCheck.tradeCheck.tradesData.count()
    lazy val totQuotes = dataCheck.quoteCheck.quoteData.count()

    //println(dataCheck.returnCheck.returnsStandardDeviation)

    //dataCheck.dataPlot.tradeDataPlot.tradesToQuoteRatioDailyPlot

    //println(dataCheck.sizeCheck.minTradeSize, dataCheck.sizeCheck.averageTradeSize,
    //  dataCheck.sizeCheck.minQuoteSize, dataCheck.sizeCheck.averageQuoteSize)


    println(dataCheck.tradeCheck.tradesPerDay.values.map(p=> 5*3600/p.toFloat).mean())
    /*
    println(dataCheck.sizeCheck.minTradeSize , dataCheck.sizeCheck.minQuoteSize, dataCheck.sizeCheck.median, dataCheck.sizeCheck.roundMultiples)
    println(dataCheck.quoteCheck.sanityChecksQuotes.numberOfZeroSPREAD)
    println(dataCheck.priceIncrementCheck.tickSize)
    println(dataCheck.priceIncrementCheck.roundMultiples)
    //dataCheck.dataPlot.tradeDataPlot.sizeHistogram(100)*/


    //println(ManageDirectory.getListOfFiles( new File("/Users/ceikit/Development/Scala")))

    /*
    dataCheck.tradingSessionCheck.summaryTradesOff.printSummary()
    dataCheck.tradingSessionCheck.summaryQuotesOff.printSummary()*/

    /*
    println("total number of Trades: " + dataCheck.tradeCheck.tradesData.count())
    println("total number of Quotes: " + dataCheck.quoteCheck.quoteData.count())
    println("number of Trades OFF session: " + tradesOff.count())
    println("number of Quotes OFF session: " + quotesOff.count())
    */

    /*
    println("number of trades: " + tradesData.tradesData.count())
    println("the number of (suspicious) trades at price 0 is: " + tradesData.numberOfZeroPrice)
    println("the number of (suspicious) trades with SIZE 0 is: " + tradesData.numberOfTradesWithSizeZero)
    println("the total number of days in the dataset is: " + tradesData.totalNumberOfDays)

    println("the total number of days with ZERO trades is: " + tradesData.daysWithZeroTrades.count())*/





  }

}