import org.apache.spark.rdd.RDD

case class DataCheckSingleAsset(tradesFile: String, quoteFile: String) {

  lazy val tradesData: RDD[(TQTimeKey, Trade)] =
    SparkCSVParsing.makeTradesArray(tradesFile).persist()

  lazy val quoteData: RDD[(TQTimeKey, Quote)] =
    SparkCSVParsing.makeQuotesArray(quoteFile).persist()

  lazy val tradeCheck: TradeCheck = TradeCheck(tradesData)
  lazy val quoteCheck: QuoteCheck = QuoteCheck(quoteData)

  def daysMatch: Boolean = { // for each Trade Day there is a Quote Day
  val boolArray =
    for{
      (tradeDay, quoteDay) <- tradeCheck.daysOfTrading.zip(quoteCheck.daysOfQuotes)
    } yield tradeDay == quoteDay
    boolArray.forall(b => b) && tradeCheck.firstDay == quoteCheck.firstDay && tradeCheck.lastDay ==  quoteCheck.lastDay
  }

  lazy val tradingSessionCheck = TradingSessionCheck(tradeCheck.tradesData, quoteCheck.quoteData)

  def dataPlot = DataPlot(tradeCheck, quoteCheck)
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


case class DataPlot(tradeCheck: TradeCheck, quoteCheck: QuoteCheck){
  def tradeDataPlot = WispDataPlotting(tradeCheck)
}


/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

case class TradingSessionCheck(tradesData: RDD[(TQTimeKey, Trade)], quoteData: RDD[(TQTimeKey, Quote)]){

  private def offSession[T <: BookInfo](bookData: RDD[(TQTimeKey, T)])  = {
    bookData
      .map{ case (key, book) =>
        val timeStamp = key.timeStamp.time
        val beforeOpening = timeStamp < "09:00"
        val afterClosing = timeStamp > "15:00:00"
        val duringLunch = timeStamp > "11:30:00" && timeStamp < "12:30"
        book match {
          case trade: Trade => TradesOffSession(key, trade, beforeOpening, afterClosing, duringLunch)
          case quote: Quote => QuotesOffSession(key, quote, beforeOpening, afterClosing, duringLunch)
        }
      }
      .filter( off => off.beforeOpening || off.afterClosing || off.duringLunch)
  }

  lazy val tradesOffSession = offSession(tradesData)
  lazy val quotesOffSession = offSession(quoteData)

}

abstract class OffSession{val beforeOpening: Boolean; val afterClosing: Boolean; val duringLunch: Boolean }

case class TradesOffSession(key:TQTimeKey, trade: Trade, beforeOpening: Boolean, afterClosing: Boolean, duringLunch: Boolean )
  extends OffSession

case class QuotesOffSession(key:TQTimeKey, trade: Quote, beforeOpening: Boolean, afterClosing: Boolean, duringLunch: Boolean )
  extends OffSession



/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


case class TradeCheck(tradesData: RDD[(TQTimeKey, Trade)]) {

  lazy val daysOfTrading = tradesData.keys.groupBy( k => k.date ).keys.collect().sorted

  lazy val tradesPerDay: RDD[(TQDate, Int)] = // daily number of Trades
    tradesData.map( t => (t._1.date, 1)).foldByKey(0)(_ + _).sortByKey()

  lazy val volumePerDayTraded: RDD[(TQDate, Double)] = // daily Volume
    tradesData.map( t => (t._1.date, t._2.size) ).foldByKey(0)(_ + _).sortByKey()

  def firstDay = daysOfTrading.head.dateToString()

  def lastDay = daysOfTrading.last.dateToString()

  def sanityChecksTrades = SanityChecksTrades(tradesData, tradesPerDay)

}




case class QuoteCheck(quoteData: RDD[(TQTimeKey, Quote)]) {

  lazy val daysOfQuotes = quoteData.keys.groupBy( k => k.date ).keys.collect().sorted

  lazy val quotesPerDay: RDD[(TQDate, Int)] = // daily number of Trades
    quoteData.map( t => (t._1.date, 1)).foldByKey(0)(_ + _).sortByKey()

  def bidLessThanAsk: Boolean = quoteData.values.filter( q => q.bid > q.ask).count() == 0

  def firstDay = daysOfQuotes.head.dateToString()

  def lastDay = daysOfQuotes.last.dateToString()

  def sanityChecksQuotes = SanityChecksQuotes(quoteData, quotesPerDay)

}

case class SanityChecksTrades(tradesData: RDD[(TQTimeKey, Trade)], tradesPerDay: RDD[(TQDate, Int)]) {

  def numberOfZeroPrice = tradesData.values.filter(t => t.tradePrice <= 0.0).count()

  def totalNumberOfDays = tradesData.keys.groupBy( k => k.date ).count()

  def numberOfTradesWithSizeZero = tradesData.values.filter(t => t.size == 0.0).count()

  def daysWithZeroTrades = tradesPerDay.filter( d => d._2 == 0).keys

  def numberOfDaysWithZeroTrades = daysWithZeroTrades.count()
}

case class SanityChecksQuotes(quoteData: RDD[(TQTimeKey, Quote)], quotesPerDay: RDD[(TQDate, Int)]) {

  def numberOfZeroQuote = quoteData.values.filter(t => t.ask <= 0 || t.bid <= 0).count()

  def totalNumberOfDays = quoteData.keys.groupBy( k => k.date ).count()

  def numberOfTradesWithSizeZero = quoteData.values.filter(t => t.askSize <= 0.0 || t.bidSize <= 0.0).count()

  def daysWithZeroTrades = quotesPerDay.filter( d => d._2 == 0).keys

  def numberOfDaysWithZeroTrades = daysWithZeroTrades.count()
}
