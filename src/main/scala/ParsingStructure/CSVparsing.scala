package ParsingStructure

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}


object SparkCSVParsing {

  val conf = new SparkConf().setMaster("local[*]").setAppName("SparkCSVParsing").set("spark.executor.memory", "4g").set("spark.driver.memory", "8g")
  val sc = new SparkContext(conf)
  val hiveContext = new HiveContext(SparkCSVParsing.sc)

  def makeTradesAndQuotesDataSet(fileName: String): RDD[(TQTimeKey, TradeAndQuote)] = {

    val file: RDD[String] = sc.textFile(fileName)
    val data: RDD[List[String]] = file.map(_.split(",").toList).filter(t => t.head != "date_xl" && t.head !="#date_xl")

    data.map(d => {
      val g: Array[String] = d.toArray
      val date = TimeFormattingUtilities.fromCSVdateToTradeDate(g(0))
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
      TQTimeKey(date, timeStamp) -> TradeAndQuote(Bid, bidSize, Ask, askSize, tradePrice, tradeSize, tradeSign)
    })
  }

  def makeTradesArray(fileName: String): RDD[(TQTimeKey, Trade)] = {

    val file: RDD[String] = sc.textFile(fileName)
    val data: RDD[List[String]] = file.map(_.split(",").toList).filter(t => t.head != "date_xl" && t.head !="#date_xl")

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
    })
      .filter{case (key, quote) => quote.ask > 0 && quote.bid > 0}
      .filter( q => q._2.bidSize > 0 && q._2.askSize > 0 )

  }

  def makeDataSet(fileName: String): RDD[TradesQuotesClass] = {
    makeTradesAndQuotesDataSet(fileName)
      .map( t => {

      val date = t._1.date
      val year =  t._1.date.year
      val month = t._1.date.month
      val day = t._1.date.dayNumber.toString
      val dayName = t._1.date.dayName
      val time = t._1.timeStamp.time
      val millisecond = t._1.timeStamp.milliseconds.toString
      val dateString =  s"$year-$month-$day"

      TradesQuotesClass(dateString, day, dayName, month, year, time, millisecond,
        t._2.bid, t._2.bidSize, t._2.ask, t._2.askSize, t._2.tradePrice, t._2.tradeSize, t._2.tradeSign)
    } )}

  def makeMinuteDataSet(fileName: String): RDD[TradesQuotesMinuteClass] = {

    def secondToMinute(s: String) = {
      val array = s.split(':')
      array(0) + ':' + array(1)
    }
    def secondString(s: String) = s.split(':').last


    makeTradesAndQuotesDataSet(fileName)
      .map( t => {

      val date = t._1.date
      val year =  t._1.date.year
      val month = t._1.date.month
      val day = t._1.date.dayNumber.toString
      val dayName = t._1.date.dayName
      val time = t._1.timeStamp.time
      val millisecond = t._1.timeStamp.milliseconds.toString
      val dateString =  s"$year-$month-$day"

      TradesQuotesMinuteClass(dateString, day, dayName, month, year, secondToMinute(time),secondString(time), millisecond,
        t._2.bid, t._2.bidSize, t._2.ask, t._2.askSize, t._2.tradePrice, t._2.tradeSize, t._2.tradeSign)
    } )}

}



object ModifiedNumericalKeyStamp {

  val conf = new SparkConf().setMaster("local[*]").setAppName("SparkCSVParsing").set("spark.executor.memory", "4g").set("spark.driver.memory", "8g")
  val sc = new SparkContext(conf)
  val hiveContext = new HiveContext(sc)

  def makeTradesAndQuotesDataSet(fileName: String): RDD[(TQTimeKeyNumerical, TradeAndQuote)] = {

    val file: RDD[String] = sc.textFile(fileName)
    val data: RDD[List[String]] = file.map(_.split(",").toList).filter(t => t.head != "date_xl" && t.head != "#date_xl")

    data.map(d => {
      val g: Array[String] = d.toArray
      val date = TimeFormattingUtilities.fromCSVdateToTradeDate(g(0))
      val numericDate = g(0).split('.')(0)
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
      TQTimeKeyNumerical(numericDate.toInt,g(1).toDouble,numericSecond,numericMillisecond,date, timeStamp) -> TradeAndQuote(Bid, bidSize, Ask, askSize, tradePrice, tradeSize, tradeSign)
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
      .groupByKey().mapValues(_.last)

  }

  def makeMinuteDataSet(fileName: String): RDD[TradesQuotesNumericalMinuteClass] = {

    def secondToMinute(s: String) = {
      val array = s.split(':')
      array(0) + ':' + array(1)
    }
    def secondString(s: String) = s.split(':').last


    makeTradesAndQuotesDataSet(fileName)
      .map( t => {

        val date = t._1.numericDate
        val day = t._1.date.dayNumber.toString
        val second = t._1.numericSecond
        val millisecond = t._1.numericMillisecond
        val time = t._1.timeStamp.time

        TradesQuotesNumericalMinuteClass(date, secondToMinute(time), second,
          millisecond, t._2.bid, t._2.bidSize, t._2.ask,
          t._2.askSize, t._2.tradePrice, t._2.tradeSize, t._2.tradeSign)
      } )}

}



