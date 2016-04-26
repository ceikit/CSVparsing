package FairPrice

import ParsingStructure.{Quote, TransformRDD}
import org.apache.spark.rdd.RDD

/**
  * Created by ceikit on 4/25/16.
  */
object FirstPriceApproximation {

  def main(args: Array[String]): Unit = {

    val matrix : Array[Array[Double]] =
      Array(
        Array(
          -0.11013963907003631,
          -0.11013963907003631,
          -0.26705023754900015,
          -0.35451162454612783,
          -0.4173492060624673),
        Array(
          -0.02110723780166127,
          -0.02110723780166127,
          -0.11877928687430823,
          -0.20214537668221738,
          -0.29769428899485895
        ),
        Array(
          -0.004210718429388989,
          -0.004210718429388989,
          -0.059073902671133764,
          -0.11395446861950141,
          -0.1901580431674684
        ),
        Array(
          7.387421092513974E-4,
          7.387421092513974E-4,
          -0.024245325791611313,
          -0.0587268271184936,
          -0.13218886433460897
        ),
        Array(
          0.0027700610074316485,
          0.0027700610074316485,
          -0.00954765165827634,
          -0.051282022064772254,
          -0.29893641507843
        ),
        Array(
          0.0043850187912256484,
          0.0043850187912256484,
          0.011078194508360178,
          0.04301369075011505,
          0.0644522261849977
        ),
        Array(
          0.005853997801345474,
          0.005853997801345474,
          0.028295658138066532,
          0.06268569175889158,
          0.13125903419452645
        ),
        Array(
          0.00684879605609193,
          0.00684879605609193,
          0.0665579060914837,
          0.11363028536278784,
          0.19292075633173758
        ),
        Array(
          0.019960522937350223,
          0.019960522937350223,
          0.1267117425539199,
          0.15999176223895925,
          0.26193405511811024
        ),
        Array(
          0.09426232514468888,
          0.09426232514468888,
          0.30471953464830887,
          0.36111885393208976,
          0.3979054039185194
        )
      )

    val quoteFile = "sashastoikov@gmail.com_5020.T_events_20130101_20131206_frequency_-1.csv.gz"
    val classUsed = FairPriceDataFrameEfficient(quoteFile)

    val delays = Array(0.01, 0.1, 1.0, 10.0, 100.0)
    val numberOfLags = delays.length

    lazy val dataSet = classUsed.makeMidPriceLater(10, delays)


    val secondApproximation: RDD[(Int, Array[Double])] =
      classUsed.gIplusOne(dataSet,numberOfLags, (x:Int, y:Int) => g(x,y,matrix))


    import ModifiedNumericalKeyStampFair.hiveContext.implicits._

    secondApproximation.toDF("bin", "array").coalesce(1).write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("codec", "org.apache.hadoop.io.compress.GzipCodec")
      .save("secondApproximation.csv")

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
