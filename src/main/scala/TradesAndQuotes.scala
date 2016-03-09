
abstract class BookInfo

case class Trade(tradePrice: Double, size: Double, tradeSign: Int) extends BookInfo

case class Quote(bid: Double, bidSize: Double, ask: Double, askSize: Double) extends BookInfo
