import org.apache.spark.rdd.RDD


class TradingParametersCheck {

}

case class LotSizeCheck(tradesData: RDD[(TQTimeKey, Trade)], quotesData: RDD[(TQTimeKey, Quote)]){

  lazy val sizeRDD = tradesData.values.map(_.size.toLong)
  lazy val minTradeSize: Double = sizeRDD.min
  lazy val maxTradeSize: Double = sizeRDD.max
  lazy val minQuoteSize: Double = quotesData.values.map( p => math.min(p.askSize, p.bidSize)).min.toInt

  lazy val roundMultiples = sizeRDD.map(s => s % minTradeSize).filter( b => b == 0 ).count() == count

  lazy val median: Double = {
    val sorted: RDD[(Long, Long)] = sizeRDD.sortBy(x => x).zipWithIndex().map { case (v, idx) => (idx, v)}
    if (count % 2 == 0) {
      val l = count / 2 - 1
      val r = l + 1
      (sorted.lookup(l).head + sorted.lookup(r).head).toDouble / 2
    } else sorted.lookup(count / 2).head.toDouble
  }

  def binnedSize(n: Int): RDD[(String, Int)] ={
    val listOfBins = binList(n)
    sizeRDD.map( s => {
      val selectedBin =
        listOfBins.filter( p => p._1 <= s.toDouble && s.toDouble <= p._2)
      selectedBin.head -> s
    } ).groupByKey().sortByKey()
      .map{case(key, value)  => (key._1.toInt.toString + '-' + key._2.toInt.toString, value.toList.length)}
      .filter{case(key, value) => value > 0}
  }

  private lazy val count = tradesData.count()
  private def binSize(n: Int): Double = (maxTradeSize - minTradeSize)/n.toDouble
  private def binList(n: Int) =
    (1 to n+1).foldLeft( List[(Double,Double)]() )((list, num) => list :+ ((num-1) * binSize(n), num * binSize(n)) )
}

case class PriceIncrementCheck(tradesData: RDD[(TQTimeKey, Trade)],quotesData: RDD[(TQTimeKey, Quote)]){


  lazy val tickSize ={
    val sorted = quotesData.sortByKey(ascending = true)
      .filter{
      q => q._2.bid != q._2.ask
    }.values.collect().par
    sorted.drop(1).zip(sorted)
      .filter{
      case (q1,q2) =>
        val bidChange = math.abs(q1.bid - q2.bid)
        val askChange = math.abs(q1.ask - q2.ask)
        val bidAskChange = math.abs(q1.ask - q1.bid)
        bidChange > 0.0 && askChange > 0.0 && bidAskChange > 0.0
     }.foldLeft(10000.0)( (min, qq) =>{
        val q1 = qq._1
        val q2 = qq._2
        val bidChange = math.abs(q1.bid - q2.bid)
        val askChange = math.abs(q1.ask - q2.ask)
        val bidAskChange = math.abs(q1.ask - q1.bid)
        math.min(math.min(math.min(bidChange, askChange),bidAskChange),min)}
       )

  }

  lazy val roundMultiples = tradesData.values.map(s => s.tradePrice % tickSize).filter( b => b == 0 ).count() == count

  private lazy val count = tradesData.count()

}

