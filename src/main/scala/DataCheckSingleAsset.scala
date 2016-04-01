import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

case class DataCheckSingleAsset(tradesFile: String, quoteFile: String) {

  val context: SparkContext = SparkCSVParsing.sc

  lazy val tradesAndQuotesData: RDD[(TQTimeKey, TradeAndQuote)] =
    SparkCSVParsing.makeTradesAndQuotesDataSet(tradesFile)

  lazy val tradesData: RDD[(TQTimeKey, Trade)] =
    SparkCSVParsing.makeTradesArray(tradesFile).persist()

  lazy val quoteData: RDD[(TQTimeKey, Quote)] =
    SparkCSVParsing.makeQuotesArray(quoteFile).persist()

  lazy val tradeCheck: TradeCheck = TradeCheck(tradesData)
  lazy val quoteCheck: QuoteCheck = QuoteCheck(quoteData)

  lazy val tradeToQuoteDailyRatio = TradeToQuoteRatioDaily(tradeCheck,quoteCheck)

  lazy val sizeCheck = LotSizeCheck(tradesData, quoteData)
  lazy val priceIncrementCheck = PriceIncrementCheck(tradesData, quoteData)

  lazy val returnCheck = Returns(tradesData)

  def daysMatch: Boolean = { // for each Trade Day there is a Quote Day
  val boolArray =
    for{
      (tradeDay, quoteDay) <- tradeCheck.daysOfTrading.zip(quoteCheck.daysOfQuotes)
    } yield {println(tradeDay, quoteDay); tradeDay == quoteDay}
    boolArray.forall(b => b) && tradeCheck.firstDay == quoteCheck.firstDay && tradeCheck.lastDay ==  quoteCheck.lastDay
  }

  lazy val tradingSessionCheck = TradingSessionCheck(tradeCheck.tradesData, quoteCheck.quoteData)

  def dataPlot = DataPlot(this)
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


case class DataPlot(dataCheck : DataCheckSingleAsset){
  def tradeDataPlot = WispDataPlotting(dataCheck)
}


/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
