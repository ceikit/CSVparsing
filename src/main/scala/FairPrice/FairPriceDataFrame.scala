package FairPrice

import ParsingStructure._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ArrayBuffer

case class FairPriceDataFrame(quoteFile: String) {

  val hiveCtx = new HiveContext(ModifiedNumericalKeyStampFair.sc)
  import hiveCtx.implicits._

  def quoteSet(): RDD[(NumericTime, Quote)] =
    ModifiedNumericalKeyStampFair.makeQuotesArray(quoteFile)
      .map{case(k,v)=> NumericTime (k.numericDate,k.numericTime,k.numericSecond, k.numericMillisecond) -> v}
      .persist()

  def makeMidPriceLater(numberOfBins : Int, dt: (Int,Int), numberOfLater: Int) = {

    def binSize(n: Int): Double = 1 / n.toDouble
    def binList(n: Int): Array[(Double, Double)] =
      (1 to n + 1)
        .foldLeft(ArrayBuffer[(Double, Double)]())((list, num) => list :+((num - 1) * binSize(n), num * binSize(n)))
        .toArray

    val listOfBins: Array[((Double, Double), Int)] = binList(numberOfBins).zipWithIndex


    val function = (q: (NumericTime, Quote)) =>
      (q._1.numericDate,q._1.numericTime,
        q._1.numericSecond,
        q._1.numericMillisecond,
        (q._2.ask + q._2.bid) / 2,
        imbalance(q._2, listOfBins))

    val dfA: DataFrame = quoteSet().map(function)
      .toDF("date", "time", "second", "milliSec", "mid", "binImbalance")


    val dfC = dfA//.filter(dfA("second") === 46260 || dfA("second") === 46261 and dfA("date") === 41278)
    //dfC.show()
    import org.apache.spark.sql.functions._

    val dfB = dfC
      .withColumnRenamed("second", "secondB")
      .withColumnRenamed("date", "dateB")
      .withColumnRenamed("milliSec", "milliSecB")
      .withColumnRenamed("mid", "midB")
      .withColumnRenamed("time", "timeB")

    //dfB.show()
    import org.apache.spark.sql.functions.udf

    val aggregateDataFrames = udf( (ls: Seq[Double], y: Double) => ls :+ y)
    val initializeColumn = udf( (x: Double) => Seq[Double](x))

    val dataFrameList: List[DataFrame] =
      (1 to numberOfLater).toList.map(i => {
       val time = dt._1 * i
        dfC.join(
        dfB, dfB("dateB") === dfC("date") and
          dfB("secondB") === dfC("second") + time  and
          dfB("timeB") - dfC("time") <=  time )
         .groupBy(dfC("date"), dfC("time"), dfC("mid"), dfC("binImbalance"),dfB("timeB"))
         .agg(max(dfB("timeB")), last( dfB("midB")))
         .select("date", "time", "mid", "binImbalance", "max(timeB)", "last(midB)()")
         .withColumnRenamed("last(midB)()", "midB")
         .withColumnRenamed("max(timeB)", "timeB")
      })

    val headList = dataFrameList.head

        dataFrameList.tail.foldLeft(
          headList
            .withColumn("laterTimesList", initializeColumn(headList("timeB")))
            .withColumn("laterMidList", initializeColumn(headList("midB")))
            .select("date", "time", "mid", "binImbalance", "laterTimesList", "laterMidList"))(
          (d1,d3) => {
          val d2 = d3
            .withColumnRenamed("date", "dateC")
            .withColumnRenamed("mid", "midC")
            .withColumnRenamed("time", "timeC")
            .withColumnRenamed("timeB", "timeD")
            .withColumnRenamed("midB", "midD")
            .withColumnRenamed("binImbalance", "binImbalanceB")


            val joined =
            d1.join(d2, d1("date") === d2("dateC") and d1("time") === d2("timeC") and d1("mid") === d2("midC"))
              .withColumnRenamed("laterTimesList", "laterTimesListB")
              .withColumnRenamed("laterMidList", "laterMidListB")


            joined
            .withColumn("laterTimesList", aggregateDataFrames(joined("laterTimesListB"),joined("timeD")))
            .withColumn("laterMidList", aggregateDataFrames(joined("laterMidListB"),joined("midD")))
            .select("date", "time", "mid", "binImbalance", "laterTimesList", "laterMidList")

        })



  }


  def imbalance(quote: Quote, array: Array[((Double, Double), Int)]) : Int = {
    val imbalance = quote.bidSize/(quote.bidSize + quote.askSize)
    TransformRDD.binnedDouble(10, imbalance, array)._2
  }

}
