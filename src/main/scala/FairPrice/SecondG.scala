package FairPrice

/**
  * Created by ceikit on 4/26/16.
  */
object SecondG {

  def main(args: Array[String]): Unit = {

    val quoteFile = "sashastoikov@gmail.com_5020.T_events_20130101_20131206_frequency_-1.csv.gz"
    val classUsed: FairPriceDataFrameEfficient = FairPriceDataFrameEfficient(quoteFile)

    val delays = Array(1.0 / 10.0, 5.0 / 10.0, 10.0, 100.0)
    val numberOfLags = delays.length
    lazy val dataSet = classUsed.makeMidPriceLater(10, delays)

  }
}
