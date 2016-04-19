package FairPrice

import org.apache.spark.rdd.RDD

/**
  * Created by ceikit on 4/11/16.
  */
object FairPriceMain {

    def main(args: Array[String]): Unit = {

      val quoteFile = "sashastoikov@gmail.com_5020.T_events_20130101_20131206_frequency_-1.csv.gz"
      val classUsed: FairPriceDataFrameEfficient = FairPriceDataFrameEfficient(quoteFile)

      lazy val dataSet = classUsed.makeMidPriceLater(10, (1, 0), 10)

      val imbalance: RDD[(Int, Array[Double])] = classUsed.conditionalExpectedValue(dataSet, 10)

      //lazy val filtered = dataSet.map(f => f.laterMids.map(_._2.isDefined)).filter( _.count(_ == true) > 5)

      //println(filtered.count())

      //val prova = dataSet.take(100).map(_.laterMids.toList).toList

      //prova.foreach(println)



      import com.quantifind.charts.Highcharts._

      imbalance.sortBy(_._1).foreach(v => {
        val bin = v._1
        line(v._2.toVector)
        val legendString = s"Imabalance bin: $bin"
        legend(List(legendString))
      })


      /*
      dataSet.toDF().coalesce(1).write
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
        .save("fairPriceAggregate.csv")*/

    }

}
