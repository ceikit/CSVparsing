

object Imbalance {

  def main(args: Array[String]): Unit = {

    val tradesFile = "sashastoikov@gmail.com_5020.T_tradesQuotes_20130103_20150909.csv"

    val quoteFile = "sashastoikov@gmail.com_5020.T_events_20130101_20131206_frequency_-1.csv.gz"

    lazy val dataCheck = DataCheckSingleAsset(tradesFile, quoteFile)

    import com.quantifind.charts.Highcharts._

    val binnedImbalance = TransformRDD.binnedRDD(100,
      dataCheck.quoteData
        .filter( f => f._1.date.month.toInt == 5)
        .values.map(q => q.bidSize/(q.bidSize+q.askSize)))

    val binnedImbalance2 = TransformRDD.binnedRDD(10,
      dataCheck.quoteData
        .filter( f => f._1.date.month.toInt == 5)
        .values.map(q => q.bidSize/(q.bidSize+q.askSize)))


    histogram(binnedImbalance.collect().toList)
    legend(List("Istogram of IMBALANCE with 100 buckets"))
    histogram(binnedImbalance2.collect().toList)
    legend(List("Istogram of IMBALANCE with 1000 buckets"))

    //line(dataCheck.quoteData.mapValues(q => ((q.bid+q.ask)/2-491.5)/(499.55-491.5)).sortByKey().values.take(3000).toList.drop(1000))




  }
}
