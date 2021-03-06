package MarketImpact

import ParsingStructure._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

/**
  * Created by ceikit on 4/8/16.
  */
case class MinuteAggregation(tradesFile: String) {

  lazy val tradesAndQuotes: RDD[TradesQuotesMinuteClass] = SparkCSVParsing.makeMinuteDataSet(tradesFile)

  val hive = SparkCSVParsing.hiveContext
  import hive.implicits._

  lazy val minuteData: RDD[(String, Map[String, Iterable[TradesQuotesMinuteClass]])] =
    tradesAndQuotes
      .map( k => k.dateString -> k).groupByKey().map{ case(day, trades) => day -> trades.groupBy(_.hour)}
      .persist()
  //.groupBy(_.dateString).map{ case(day, trades) => day -> trades.groupBy(_.hour)}.persist()

  lazy val tradedVolume =
    minuteData.flatMapValues(_.mapValues(tradedVolumeMinute))
      .map{case(s1,v) => Aux(s1,v._1, v._2)}

  lazy val tradeFlow: RDD[Aux] =
    minuteData.flatMapValues(_.mapValues(tradesFlowMinute))
      .map{case(s1,v) => Aux(s1,v._1, v._2)}

  lazy val averageSpread =
    minuteData.flatMapValues(_.mapValues(averageMinuteSpread))
      .map{case(s1,v) => Aux(s1,v._1, v._2)}

  lazy val realizedVariance =
    minuteData.flatMapValues(_.mapValues(v => secondVarianceOfReturn(minuteToSecond(v))))
      .map{case(s1,v) => Aux(s1,v._1, v._2)}

  lazy val returns = minuteData.flatMapValues(returnsMinute)
    .map{case(s1,v) => Aux(s1,v._1, v._2)}



  def tradedVolumeMinute(minuteData: Iterable[TradesQuotesMinuteClass]) : Double =
    minuteData.foldLeft(0.0)((d,minute) => d + minute.tradeSize)

  def numberOfTradesMinute(minuteData: Iterable[TradesQuotesMinuteClass]) : Int = minuteData.size

  def tradesFlowMinute(minuteData: Iterable[TradesQuotesMinuteClass]) : Double =
    minuteData.foldLeft(0.0)((d,minute) => d + minute.tradeSize * minute.tradeSign)

  def averageMinuteSpread(minuteData: Iterable[TradesQuotesMinuteClass]) : Double =
    minuteData.foldLeft(0.0)((d,minute) => d + 2 * (minute.ask - minute.bid)/(minute.ask + minute.bid)) /
      minuteData.size.toDouble


  def secondVarianceOfReturn(secondData: Map[Int, Iterable[TradesQuotesMinuteClass]]) : Double = {
    val midPriceSecondMap: Array[(Double, Int)] =
      secondData
        .mapValues(
          t => {
            val trade = t.maxBy(_.millisecond)
            (trade.ask + trade.bid)/2}
        ).toArray.sortBy(_._1).map(_._2).zipWithIndex// map MID PRICE

    midPriceSecondMap.map{
      case(value, i) =>
        i match {
          case 0 =>  0.0
          case j => (value - midPriceSecondMap(i-1)._1 ) / midPriceSecondMap(i-1)._1
        }
    }     // compute returns
      .foldLeft(0.0)((prev, value) => prev + value * value) // compute sum of squared returns, aka variance

  }


  def returnsMinute(hourData: Map[String, Iterable[TradesQuotesMinuteClass]]): Map[String, Double] = {

    val indexedData: Array[((String, Double), Int)] =
      hourData.mapValues(lastMinuteMidPrice)
        .toArray.sortBy(_._1)
        .zipWithIndex

    indexedData.map{
      case( v, i )=>
        i match {
          case 0 => v._1 -> 0.0
          case j => v._1 ->  ( v._2 - indexedData(j-1)._1._2 ) / indexedData(j-1)._1._2
        }}.toMap
  }

  def minuteToSecond(minuteData: Iterable[TradesQuotesMinuteClass]) : Map[Int, Iterable[TradesQuotesMinuteClass]] =
    minuteData.groupBy(m => m.second.toInt)

  def lastMinuteMidPrice( minuteData: Iterable[TradesQuotesMinuteClass]) : Double = {
    val lastTrade = minuteToSecond(minuteData).maxBy(_._1)._2.maxBy(_.millisecond)
    (lastTrade.bid + lastTrade.ask) / 2
  }


  def makeMinuteAggregateData(): Dataset[AggregateMinuteData] = {

    val volume = tradedVolume.toDF("dateString","hourV", "tradedVolume")
    val flow = tradeFlow.toDF("dateStringF", "hour", "tradeFlow")
    val spread = averageSpread.toDF("dateStringS","hour",  "averageSpread")
    val variance = realizedVariance.toDF("dateStringV","hour",  "realizedVariance")
    val gain = returns.toDF("dateStringG", "hour", "returns")

    volume
      .join(flow, volume("dateString") === flow("dateStringF") and volume("hourV") === flow("hour"))
      .join(spread, volume("dateString") === spread("dateStringS")and volume("hourV") === spread("hour"))
      .join(variance, volume("dateString") === variance("dateStringV")and volume("hourV") === variance("hour"))
      .join(gain, volume("dateString") === gain("dateStringG")and volume("hourV") === gain("hour"))
      .select("dateString","hourV", "tradedVolume","tradeFlow","averageSpread","realizedVariance","returns")
      .map(r =>
        AggregateMinuteData(
          r(0).toString,
          r(1).toString,
          r(2).asInstanceOf[Double],
          r(3).asInstanceOf[Double],
          r(4).asInstanceOf[Double],
          r(5).asInstanceOf[Double],
          r(6).asInstanceOf[Double])
      ).toDS()


  }
}


case class NumericMinuteAggregation(tradesFile: String) {

  lazy val tradesAndQuotes: RDD[TradesQuotesNumericalMinuteClass] = ModifiedNumericalKeyStamp.makeMinuteDataSet(tradesFile)

  lazy val minuteData: RDD[(Int, Map[String, Iterable[TradesQuotesNumericalMinuteClass]])] =
    tradesAndQuotes
      .map( k => k.date -> k).groupByKey().map{ case(day, trades) => day -> trades.groupBy(_.hour)}
      .persist()


  def makeMinuteAggregateData(): RDD[AggregateNumericalMinuteData] = {

    minuteData.flatMapValues(f => {
      val withoutReturn: Map[String, (Int, String, Double) => AggregateNumericalMinuteData] =
        f.mapValues(
          m => {
            (day: Int, minute: String, r: Double) =>
              AggregateNumericalMinuteData(day,minute,
                tradedVolumeMinute(m),
                tradesFlowMinute(m),
                averageMinuteSpread(m),
                secondVarianceOfReturn(minuteToSecond(m)), r)}
        )
      returnsMinute(f).map{ case(k,r) => (d:Int) => withoutReturn.apply(k).apply(d,k,r)}
    }).map{case(day, f) => f.apply(day)}

  }

  def tradedVolumeMinute(minuteData: Iterable[TradesQuotesNumericalMinuteClass]) : Double =
    minuteData.foldLeft(0.0)((d,minute) => d + minute.tradeSize)

  def numberOfTradesMinute(minuteData: Iterable[TradesQuotesNumericalMinuteClass]) : Int = minuteData.size

  def tradesFlowMinute(minuteData: Iterable[TradesQuotesNumericalMinuteClass]) : Double =
    minuteData.foldLeft(0.0)((d,minute) => d + minute.tradeSize * minute.tradeSign)

  def averageMinuteSpread(minuteData: Iterable[TradesQuotesNumericalMinuteClass]) : Double =
    minuteData.foldLeft(0.0)((d,minute) => d + 2 * (minute.ask - minute.bid)/(minute.ask + minute.bid)) /
      minuteData.size.toDouble


  def secondVarianceOfReturn(secondData: Map[Int, Iterable[TradesQuotesNumericalMinuteClass]]) : Double = {
    val midPriceSecondMap: Array[(Double, Int)] =
      secondData
        .mapValues(
          t => {
            val trade = t.maxBy(_.millisecond)
            (trade.ask + trade.bid)/2}
        ).toArray.sortBy(_._1).map(_._2).zipWithIndex// map MID PRICE

    midPriceSecondMap.map{
      case(value, i) =>
        i match {
          case 0 =>  0.0
          case j => (value - midPriceSecondMap(i-1)._1 ) / midPriceSecondMap(i-1)._1
        }
    }     // compute returns
      .foldLeft(0.0)((prev, value) => prev + value * value) // compute sum of squared returns, aka variance

  }


  def returnsMinute(hourData: Map[String, Iterable[TradesQuotesNumericalMinuteClass]]): Map[String, Double] = {

    val indexedData: Array[((String, Double), Int)] =
      hourData.mapValues(lastMinuteMidPrice)
        .toArray.sortBy(_._1)
        .zipWithIndex

    indexedData.map{
      case( v, i )=>
        i match {
          case 0 => v._1 -> 0.0
          case j => v._1 ->  ( v._2 - indexedData(j-1)._1._2 ) / indexedData(j-1)._1._2
        }}.toMap
  }

  def minuteToSecond(minuteData: Iterable[TradesQuotesNumericalMinuteClass]) : Map[Int, Iterable[TradesQuotesNumericalMinuteClass]] =
    minuteData.groupBy(m => m.second)

  def lastMinuteMidPrice( minuteData: Iterable[TradesQuotesNumericalMinuteClass]) : Double = {
    val lastTrade = minuteToSecond(minuteData).maxBy(_._1)._2.maxBy(_.millisecond)
    (lastTrade.bid + lastTrade.ask) / 2
  }



}
