import org.apache.spark.rdd.RDD

//////////////////////////////////////////////////

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

  def numberOfZeroSPREAD = quoteData.filter( q => q._2.bid == q._2.ask ).count()

}