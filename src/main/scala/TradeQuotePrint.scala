


/**
 * Created by ceikit on 3/4/16.
 */
object TradeQuotePrint {

  def nicePrintln(tradeAndQuote: TradeAndQuote ): Unit = {
    val timeKey = tradeAndQuote._1
    val trade = tradeAndQuote._2._1
    val quote = tradeAndQuote._2._2.get
    println( timeKey.date.year + ' ' + timeKey.date.month + ' ' + timeKey.date.dayNumber + ' ' + timeKey.date.dayName +
      ' ' + timeKey.timeStamp.time + ' ' + timeKey.timeStamp.milliseconds + "ms " +
      " | " + "trade PRICE: " + trade.tradePrice +  " | " + "trade SIZE: " + trade.size +
      " | " + "trade SIGN: " + trade.tradeSign +  " ||| " +
      "BID: " + quote.bid +  " | "+ "bid Size: " + quote.bidSize +
      " | " + "ASK: " + quote.bid +  " | " + "ask Size: " + quote.askSize)
  }
  type TradeAndQuote = (TQTimeKey, (Trade, Option[Quote]))
}


