import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object SparkCSVParsing {

  val conf = new SparkConf().setMaster("local[*]").setAppName("SparkCSVParsing").set("spark.executor.memory", "4g").set("spark.driver.memory", "8g")
  val sc = new SparkContext(conf)

  def makeTradesArray(fileName: String): RDD[(TQTimeKey, Trade)] = {

    val file: RDD[String] = sc.textFile(fileName)
    val data = file.map(_.split(",").toList).filter(t => t.head != "date_xl" && t.head !="#date_xl")

    data.map(d => {
      val g: Array[String] = d.toArray
      val date = TimeFormattingUtilities.fromCSVdateToTradeDate(g(0))
      val timeStamp = TimeFormattingUtilities.fromCSVtimeStampToTradeTimeStamp(g(1))
      val tradePrice = g(2).toDouble
      val size = g(3).toDouble
      val tradeSign = g(4).toDouble.toInt
      TQTimeKey(date, timeStamp) -> Trade(tradePrice, size, tradeSign)
    })
  }

  def makeQuotesArray(fileName: String): RDD[(TQTimeKey, Quote)] = {

    val file = sc.textFile(fileName)
    val data = file.map(_.split(",").toList).filter( d => d.head != "date_xl" && d.head !="#date_xl" )

    data.map(d => {
      val g: Array[String] = d.toArray
      val date = TimeFormattingUtilities.fromCSVdateToTradeDate(g(0))
      val timeStamp = TimeFormattingUtilities.fromCSVtimeStampToTradeTimeStamp(g(1))
      val bid = g(2).toDouble
      val bidSize = g(3).toDouble
      val ask = g(4).toDouble
      val askSize = g(5).toDouble
      TQTimeKey(date, timeStamp) -> Quote(bid, bidSize, ask, askSize)
    }).filter{case (key, quote) => quote.ask > 0 && quote.bid > 0}

  }

}

