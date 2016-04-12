package FairPrice

/**
  * Created by ceikit on 4/11/16.
  */
object FairPriceMain {


    def main(args: Array[String]): Unit = {

      val tradesFile = "sashastoikov@gmail.com_7203.T_tradesQuotes_20130103_20150909.csv.gz"
      val quoteFile = "sashastoikov@gmail.com_7203.T_events_20130103_20130706_frequency_-1.csv.gz"
      val classUsed = FairPriceDataFrame(quoteFile)

      val dataSet = classUsed.makeMidPriceLater(10, (1, 0), 10)

      dataSet.coalesce(1).write
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
        .save("fairPriceAggregate.csv")

    }

}
