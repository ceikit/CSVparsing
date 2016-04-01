
abstract class BookInfo

case class Trade(tradePrice: Double, size: Double, tradeSign: Int) extends BookInfo

case class Quote(bid: Double, bidSize: Double, ask: Double, askSize: Double) extends BookInfo

case class TradeAndQuote(bid: Double, bidSize: Double,
                         ask: Double, askSize: Double,
                         tradePrice: Double, tradeSize: Double, tradeSign: Int)


case class TradesQuotesClass(dateString: String,
                             day: String,
                             dayName:String,
                             month: String,
                             year: String,
                             hour: String,
                             millisecond: String,
                             bid: Double,
                             bidSize: Double,
                             ask: Double,
                             askSize: Double,
                             tradePrice: Double,
                             tradeSize: Double,
                             tradeSign: Int)



//Traded volume (sum of trade sizes)
// Number of trades (count of trades)
// Trade flow (sum of trade sizes multiplied by trade signs)
// Average depth (average of bid size and ask size)
// Average spread (average of bid‐ask spread / mid price)
// Realized variance (sum of squared mid‐quote returns, sampled over 60 one‐
//second intervals within each minute)
// Return (from last mid‐quote of previous minute to last mid‐quote of this minute)