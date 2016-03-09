import org.apache.spark.rdd.RDD

/**
 * Created by ceikit on 3/8/16.
 */
case class DataCheckSingleAsset(tradesFile: String, quoteFile: String) {

  val tradesData: RDD[(TQTimeKey, Trade)] =
    SparkCSVParsing.makeTradesArray(tradesFile).persist()

  val quoteData: RDD[(TQTimeKey, Quote)] =
    SparkCSVParsing.makeQuotesArray(quoteFile).persist()

  lazy val tradeCheck: TradeCheck = TradeCheck(tradesData)
  lazy val quoteCheck: QuoteCheck = QuoteCheck(quoteData)

  def daysMatch: Boolean = { // for each Trade Day there is a Quote Day
  val boolArray = for{
      (tradeDay, quoteDay) <- tradeCheck.daysOfTrading.zip(quoteCheck.daysOfQuotes)
    } yield tradeDay == quoteDay
    boolArray.forall(b => b) && tradeCheck.firstDay == quoteCheck.firstDay && tradeCheck.lastDay ==  quoteCheck.lastDay
  }

  lazy val tradingSessionCheck = TradingSessionCheck(tradeCheck.tradesData, quoteCheck.quoteData)

  def dataPlot = DataPlot(tradeCheck, quoteCheck)
}



case class DataPlot(tradeCheck: TradeCheck, quoteCheck: QuoteCheck){
  def tradeDataPlot = WispDataPlotting(tradeCheck)
}




case class TradingSessionCheck(tradesData: RDD[(TQTimeKey, Trade)], quoteData: RDD[(TQTimeKey, Quote)]){

  def tradesOffSession: RDD[String] = tradesData.keys
    .map{_.timeStamp.time}
    .filter{timeStamp =>
    val outsideTradingHours = timeStamp < "09:00" || timeStamp > "15:00:00"
    val duringLunch = timeStamp > "11:30:00" && timeStamp < "12:30"
    outsideTradingHours || duringLunch}

  def quotesOffSession = quoteData.keys
    .map{_.timeStamp.time}
    .filter{timeStamp =>
    val outsideTradingHours = timeStamp < "09:00" || timeStamp > "15:00:00"
    val duringLunch = timeStamp > "11:30:00" && timeStamp < "12:30"
    outsideTradingHours || duringLunch}

}

case class TradeCheck(tradesData: RDD[(TQTimeKey, Trade)]) {

  lazy val daysOfTrading = tradesData.keys.groupBy( k => k.date ).keys.collect().sorted

  def firstDay = daysOfTrading.head.dateToString()

  def lastDay = daysOfTrading.last.dateToString()

  lazy val tradesPerDay: RDD[(TQDate, Int)] = // daily number of Trades
    tradesData.map( t => (t._1.date, 1)).foldByKey(0)(_ + _).sortByKey()

  lazy val volumePerDayTraded: RDD[(TQDate, Double)] = // daily Volume
    tradesData.map( t => (t._1.date, t._2.size) ).foldByKey(0)(_ + _).sortByKey()

  // SANITY CHECKS

  def sanityChecksTrades = SanityChecksTrades(tradesData, tradesPerDay)

}

case class SanityChecksTrades(tradesData: RDD[(TQTimeKey, Trade)], tradesPerDay: RDD[(TQDate, Int)]) {

  def numberOfZeroPrice = tradesData.values.filter(t => t.tradePrice <= 0.0).count()

  def totalNumberOfDays = tradesData.keys.groupBy( k => k.date ).count()

  def numberOfTradesWithSizeZero = tradesData.values.filter(t => t.size == 0.0).count()

  def daysWithZeroTrades = tradesPerDay.filter( d => d._2 == 0).keys

  def numberOfDaysWithZeroTrades = daysWithZeroTrades.count()
}



case class QuoteCheck(quoteData: RDD[(TQTimeKey, Quote)]) {

  def bidLessThanAsk: Boolean = quoteData.values.filter( q => q.bid > q.ask).count() == 0

  def daysOfQuotes = quoteData.keys.groupBy( k => k.date ).keys.collect().sorted

  def firstDay = daysOfQuotes.head.dateToString()

  def lastDay = daysOfQuotes.last.dateToString()

  def quotesPerDay: RDD[(TQDate, Int)] = // daily number of Trades
    quoteData.map( t => (t._1.date, 1)).foldByKey(0)(_ + _).sortByKey()

  def sanityChecksQuotes = SanityChecksQuotes(quoteData, quotesPerDay)

}

case class SanityChecksQuotes(quoteData: RDD[(TQTimeKey, Quote)], quotesPerDay: RDD[(TQDate, Int)]) {

  def numberOfZeroQuote = quoteData.values.filter(t => t.ask <= 0 || t.bid <= 0).count()

  def totalNumberOfDays = quoteData.keys.groupBy( k => k.date ).count()

  def numberOfTradesWithSizeZero = quoteData.values.filter(t => t.askSize <= 0.0 || t.bidSize <= 0.0).count()

  def daysWithZeroTrades = quotesPerDay.filter( d => d._2 == 0).keys

  def numberOfDaysWithZeroTrades = daysWithZeroTrades.count()
}
