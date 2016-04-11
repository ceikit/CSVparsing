package Checks
import ParsingStructure._
import org.apache.spark.rdd.RDD


case class TradeToQuoteRatioDaily(tradeCheck: TradeCheck, quoteCheck: QuoteCheck){
  lazy val dailyTradeToQuoteRatio: RDD[(TQDate, Float)] =
    tradeCheck.tradesPerDay.join(quoteCheck.quotesPerDay)
      .mapValues{case(tradesNumber,quotesNumber)=>quotesNumber.toFloat/tradesNumber.toFloat }
}

case class Returns(tradesData: RDD[(TQTimeKey, Trade)]){

  lazy val dailyCloseTrades: RDD[(TQDate, Trade)] =
    tradesData
      .map{v => v._1.date -> v} // RDD[(TQDate, (TQTimeKey, Trade))]
      .groupByKey() // grouped by DAY : RDD[(TQDate, Iterable[(TQTimeKey, Trade)])]
      .mapValues( t => t.toList.sortBy(_._1).last)
      .map{case(date, keyValue) => date -> keyValue._2} // closing trade: RDD[(TQDate, Trade)]

  lazy val dailyCloseReturns: RDD[(TQDate, Double)] = {

    val sortedTrades: RDD[(Long, (TQDate, Double))] =
      dailyCloseTrades.mapValues( _.tradePrice).sortByKey(ascending = true).zipWithIndex()
        .map{case(value,index) => index -> value}

    val previousTrade = sortedTrades.collect()

    sortedTrades.map{
      case(i, value) =>
        i match {
          case 0 => value._1 -> 0.0
          case _ => value._1 -> priceReturn(previousTrade(i.toInt-1)._2._2,value._2)}
    }
  }

  lazy val returnsStandardDeviation: Double = dailyCloseReturns.values.sampleStdev()

  private def priceReturn(prevPrice: Double, nextPrice: Double) = (nextPrice - prevPrice) / prevPrice
}
