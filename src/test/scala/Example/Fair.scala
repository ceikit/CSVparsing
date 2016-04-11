package Example

import FairPrice.FairPriceDataFrame

/**
  * Created by ceikit on 4/11/16.
  */
object Fair {

  def main(args: Array[String]): Unit = {

    val tradesFile = "sashastoikov@gmail.com_7203.T_tradesQuotes_20130103_20150909.csv.gz"
    val quoteFile = "sashastoikov@gmail.com_7203.T_events_20130103_20130706_frequency_-1.csv.gz"


    val dataSet = FairPriceDataFrame(quoteFile).makeMidPriceAux(10, (2, 0), 3)
  }



}
