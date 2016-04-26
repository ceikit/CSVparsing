package FairPrice

import org.apache.spark.rdd.RDD

/**
  * Created by ceikit on 4/11/16.
  */
object FairPriceGfunction {

    def main(args: Array[String]): Unit = {

      val quoteFile = "sashastoikov@gmail.com_5020.T_events_20130101_20131206_frequency_-1.csv.gz"
      val classUsed: FairPriceDataFrameEfficient = FairPriceDataFrameEfficient(quoteFile)

      val delays = Array(1.0/10.0, 5.0/10.0, 10.0, 100.0 )
      val numberOfLags = delays.length

      lazy val dataSet = classUsed.makeMidPriceLater(10, delays)

      val imbalance: RDD[(Int, Array[Double])] = classUsed.g1(dataSet, numberOfLags)


      import ModifiedNumericalKeyStampFair.hiveContext.implicits._
      /*
      imbalance.sortBy(_._1).foreach(v => {
        val bin = v._1
        println(v._1, v._2.toList)
        line(0.0 +: delays.toList, 0.0 +: v._2.toVector)
        xAxisType("logarithmic")
        legend(List(s"Imbalance bin: $bin"))
      })*/



      imbalance.toDF("bin", "array").coalesce(1).write
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
        .save("imbalance.csv")

    }

}
