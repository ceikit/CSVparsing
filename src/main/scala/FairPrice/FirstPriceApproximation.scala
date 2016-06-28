package FairPrice

import ParsingStructure.{Quote, TransformRDD}
import org.apache.spark.rdd.RDD

/**
  * Created by ceikit on 4/25/16.
  */
object FirstPriceApproximation {

  def main(args: Array[String]): Unit = {


    val quoteFile = "sashastoikov@gmail.com_8604.T_events_20130103_20130706_frequency_-1.csv.gz"
    val classUsed = FairPriceDataFrameEfficient(quoteFile)

    val delays = Vector(0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0 )
    val numberOfLags = delays.length

    val numberOfImbalanceBins = 100


    lazy val dataSet: RDD[(Int, FairDataFrame)] = classUsed.makeMidPriceLater(numberOfImbalanceBins, delays)

    //dataSet.values.toDF().show
    //val secondApproximation: RDD[(Int, Array[Double])] =
      //classUsed.gIplusOne(dataSet,numberOfLags, (x:Int, y:Int) => g(x,y,matrix))


//    dataSet.groupByKey().foreach { f =>
//      ModifiedNumericalKeyStampFair.sc.parallelize(f._2.toSeq).toDF().write
//      .format("com.databricks.spark.csv")
//        .option("header", "true")
//        .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
//        .save("nomuraMids" + f._1)
//    }

    /*
    dataSet.values.toDF().coalesce(dataSet.partitions.length).write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .save("nomuraMids2")*/



    /*
    secondApproximation.toDF("bin", "array").coalesce(1).write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .save("secondApproximation.csv")*/

    /*

    val firstPrice =
      classUsed.quoteSet
        .map( q => q._1 -> aux(q._2,10))
        .map(k => k._1 ->  (k._2._1 +: (1 to numberOfLags).map(n => k._2._1 + g(k._2._2,n-1, matrix))))
        .sortBy(_._1).take(1000).map(_._2.toArray).toList

    import com.quantifind.charts.Highcharts._


    val mid = firstPrice.map( k => k(0))
    val oneDt = firstPrice.map( k => k(1))
    val threeDt =  firstPrice.map( k => k(2))
    val fourDt =  firstPrice.map( k => k(3))
    val fiveDt =  firstPrice.map( k => k(4))
    line(mid)
    hold
    line(oneDt)
    hold()
    line(threeDt)
    hold()
    line(fourDt)
    hold()
    line(fiveDt)
    legend(List("mid","first", "third", "fourth", "fifth"))*/


  }


  def aux(q: Quote, numberOfBins: Int): (Double, Int) = {


    def imbalance(quote: Quote, array: Array[((Double, Double), Int)]): Int = {
      val imbalance = quote.bidSize / (quote.bidSize + quote.askSize)
      TransformRDD.binnedDouble(imbalance, array)._2
    }

    (q.ask + q.bid) / 2 -> imbalance(q, ImbalanceUtilities.listOfBins(10))
  }




  def g(imbalance: Int, dt: Int, matrix: Array[Array[Double]]): Double = {

    matrix(imbalance)(dt)
  }

}
