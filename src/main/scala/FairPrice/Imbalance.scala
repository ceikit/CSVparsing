package FairPrice

import ParsingStructure._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ArrayBuffer


object Imbalance {

  //    val tradesFile = "sashastoikov@gmail.com_FGBSH3_tradesQuotes_20130103_20130707.csv.gz"
  //    val quoteFile = "sashastoikov@gmail.com_FGBSH3_events_20130101_20130707_frequency_-1.csv.gz"
  // sashastoikov@gmail.com_7203.T_tradesQuotes_20130103_20150909.csv
  //sashastoikov@gmail.com_8604.T_tradesQuotes_20130103_20150909.csv

  val tradesFile = "sashastoikov@gmail.com_7203.T_tradesQuotes_20130103_20150909.csv.gz"

  val hiveCtx = new HiveContext(SparkCSVParsing.sc)
  import Imbalance.hiveCtx.implicits._

  lazy val tradesAndQuotes: Dataset[TradesQuotesClass] = SparkCSVParsing.makeDataSet(tradesFile).toDS()


  lazy val coder: (Double, Double, Array[((Double, Double), Int)]) => Int =
    (bidSize: Double, askSize: Double, array: Array[((Double, Double), Int)]) => {
    val imbalance = bidSize/(bidSize + askSize)
    TransformRDD.binnedDouble(imbalance, array)._2
  }

  def makeVolumeImbalance(data: Dataset[TradesQuotesClass], tradeSign: Int, n: Int): Dataset[(Int, Long)] = {
    def binSize(n: Int): Double = 1/n.toDouble
    def binList(n: Int): Array[(Double, Double)] =
      (1 to n+1)
        .foldLeft( ArrayBuffer[(Double,Double)]() )((list, num) => list :+ ((num-1) * binSize(n), num * binSize(n)) )
        .toArray

    val listOfBins: Array[((Double, Double), Int)] = binList(n).zipWithIndex

    tradeSign match {
      case -1 => data
        .filter(_.tradeSign == tradeSign)
        .map( q => coder(q.bidSize,q.askSize,listOfBins) -> q.tradeSize )
        .groupBy( _._1).count()//.reduce((x,y) => x._1 -> (x._2 + y._2))
      //.map(_._2)
      case 1 => data
        .filter(_.tradeSign == tradeSign)
        .map( q => coder(q.bidSize,q.askSize,listOfBins) -> q.tradeSize )
        .groupBy( _._1).count()//.reduce((x,y) => x._1 -> (x._2 + y._2))
      //.map(_._2)
    }
  }

  lazy val sellVolume: Dataset[(Int, Long)] = makeVolumeImbalance(tradesAndQuotes, -1, 1000)
  lazy val buyVolume = makeVolumeImbalance(tradesAndQuotes, 1, 1000)

  def volumeToImbalancePlot = {

    import com.quantifind.charts.Highcharts._

    line(sellVolume.collect().toList.sortBy(_._1) )
    hold()
    line(buyVolume.collect().toList.sortBy(_._1) )
    legend(List("Sell Volume", "Buy Volume"))
  }



}
