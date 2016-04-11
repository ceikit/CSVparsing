package FairPrice
import ParsingStructure._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
object ModifiedNumericalKeyStamp {

  val conf = new SparkConf().setMaster("local[*]").setAppName("SparkCSVParsing").set("spark.executor.memory", "4g").set("spark.driver.memory", "8g")
  val sc = new SparkContext(conf)

  def makeTradesAndQuotesDataSet(fileName: String): RDD[(TQTimeKeyNumerical, TradeAndQuote)] = {

    val file: RDD[String] = sc.textFile(fileName)
    val data: RDD[List[String]] = file.map(_.split(",").toList).filter(t => t.head != "date_xl" && t.head != "#date_xl")

    data.map(d => {
      val g: Array[String] = d.toArray
      val date = TimeFormattingUtilities.fromCSVdateToTradeDate(g(0))
      val numericSecond = g(1).split('.')(0).toInt
      val numericMillisecond =  g(1).split('.')(1).toInt
      val timeStamp = TimeFormattingUtilities.fromCSVtimeStampToTradeTimeStamp(g(1))
      val Bid = g(2).toDouble
      val bidSize = g(3).toDouble
      val Ask = g(4).toDouble
      val askSize = g(5).toDouble
      val tradePrice = g(6).toDouble
      val tradeSize = g(7).toDouble
      val tradeSign = tradePrice match {
        case Bid => -1
        case Ask => 1
        case _ => 0
      }
      TQTimeKeyNumerical(g(0).toInt,g(1).toDouble,numericSecond,numericMillisecond,date, timeStamp) -> TradeAndQuote(Bid, bidSize, Ask, askSize, tradePrice, tradeSize, tradeSign)
    })
  }


  def makeQuotesArray(fileName: String): RDD[(TQTimeKeyNumerical, Quote)] = {

    val file = sc.textFile(fileName)
    val data = file.map(_.split(",").toList).filter( d => d.head != "date_xl" && d.head !="#date_xl" )

    data.map(d => {
      val g: Array[String] = d.toArray
      val numericDate = g(0).split('.').head.toInt
      val numericSecond = g(1).split('.')(0).toInt
      val numericMillisecond =  g(1).split('.')(1).toInt
      val date = TimeFormattingUtilities.fromCSVdateToTradeDate(g(0))
      val timeStamp = TimeFormattingUtilities.fromCSVtimeStampToTradeTimeStamp(g(1))
      val bid = g(2).toDouble
      val bidSize = g(3).toDouble
      val ask = g(4).toDouble
      val askSize = g(5).toDouble
      TQTimeKeyNumerical(numericDate,g(1).toDouble,numericSecond,numericMillisecond,date, timeStamp) ->
        Quote(bid, bidSize, ask, askSize)
    })
      .filter{case (key, quote) => quote.ask > 0 && quote.bid > 0}
      .filter( q => q._2.bidSize > 0 && q._2.askSize > 0 )

  }
}