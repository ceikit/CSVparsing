import Checks.DataCheckSingleAsset

object PriceIncrement {

  def main(args: Array[String]): Unit ={

    val tradesFile ="sashastoikov@gmail.com_7203.T_tradesQuotes_20130103_20150909.csv.gz"
    val quoteFile ="sashastoikov@gmail.com_7203.T_events_20130103_20130706_frequency_-1.csv.gz"

    lazy val dataCheck = DataCheckSingleAsset(tradesFile, quoteFile)

    val tickSize = dataCheck.priceIncrementCheck.tickSize

    println(tickSize)

    println(dataCheck.quoteData.values.map(q => q.ask - q.bid).mean())
    println(dataCheck.quoteData.values.filter(q => math.abs(q.ask - q.bid - tickSize)> 0.001).count(), dataCheck.quoteData.count())
//    println(dataCheck.priceIncrementCheck.roundMultiples)
  }

}
