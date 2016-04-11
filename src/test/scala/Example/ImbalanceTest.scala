package Example

import Checks.DataCheckSingleAsset
import ParsingStructure.TransformRDD
import org.apache.spark.rdd.RDD

object ImbalanceTest {

  def main(args: Array[String]): Unit = {

//    val tradesFile = "sashastoikov@gmail.com_FGBSH3_tradesQuotes_20130103_20130707.csv.gz"
//    val quoteFile = "sashastoikov@gmail.com_FGBSH3_events_20130101_20130707_frequency_-1.csv.gz"

    val tradesFile ="sashastoikov@gmail.com_7203.T_tradesQuotes_20130103_20150909.csv.gz"
    val quoteFile ="sashastoikov@gmail.com_7203.T_events_20130103_20130706_frequency_-1.csv.gz"

    lazy val dataCheck = DataCheckSingleAsset(tradesFile, quoteFile)

    import com.quantifind.charts.Highcharts._

    val binnedImbalance: RDD[(String, Int)] = TransformRDD.binnedRDD(100,
      dataCheck.quoteData
        .filter( q => q._2.ask - q._2.bid == 5.0)
        .values.map(q => q.bidSize/(q.bidSize+q.askSize)))

    val binnedImbalance2 = TransformRDD.binnedRDD(1000,
      dataCheck.quoteData
        //.filter( f => f._1.date.month.toInt == 2)
        .filter( q => q._2.ask - q._2.bid == 5.0)
        .values.map(q => q.bidSize/(q.bidSize+q.askSize)))


    histogram(binnedImbalance.collect().toList)
    legend(List("Istogram of IMBALANCE with 100 buckets"))
    histogram(binnedImbalance2.collect().toList)
    legend(List("Istogram of IMBALANCE with 1000 buckets"))

    //line(dataCheck.quoteData.mapValues(q => ((q.bid+q.ask)/2-491.5)/(499.55-491.5)).sortByKey().values.take(3000).toList.drop(1000))



  }
}
