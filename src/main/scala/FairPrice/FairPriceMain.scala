package FairPrice

import org.apache.spark.rdd.RDD

/**
  * Created by ceikit on 4/11/16.
  */
object FairPriceMain {

  def main(args: Array[String]): Unit = {

    val quoteFile = "sashastoikov@gmail.com_5020.T_events_20130101_20131206_frequency_-1.csv.gz"
    val classUsed: FairPriceDataFrameEfficient = FairPriceDataFrameEfficient(quoteFile)

    val delays = Array(1.0/10.0, 5.0/10.0, 10.0, 100.0 )
    val numberOfLags = delays.length

    lazy val dataSet = classUsed.makeMidPriceLater(10, delays)

    val imbalance: RDD[(Int, Array[Double])] = classUsed.g1(dataSet, numberOfLags)




    import com.quantifind.charts.Highcharts._

    imbalance.sortBy(_._1).foreach(v => {
      val bin = v._1
      line(v._2.toVector)
      val legendString = s"Imbalance bin: $bin"
      legend(List(legendString))
    })



  }

}