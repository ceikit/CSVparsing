package FairPrice

import ParsingStructure._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext

import scala.collection.mutable.ArrayBuffer

case class FairPriceDataFrameEfficient(quoteFile: String) {

  val hiveCtx = ModifiedNumericalKeyStampFair.hiveContext

  def quoteSet: RDD[(NumericTime, Quote)] =
    ModifiedNumericalKeyStampFair.makeQuotesArray(quoteFile)
      .map { case (k, v) => NumericTime(k.numericDate, k.numericTime, k.numericSecond, k.numericMillisecond) -> v }


  def makeMidPriceLater(numberOfBins: Int, dt: (Int, Int), numberOfLater: Int) = {

    def binSize(n: Int): Double = 1 / n.toDouble
    def binList(n: Int): Array[(Double, Double)] =
      (1 to n + 1)
        .foldLeft(ArrayBuffer[(Double, Double)]())((list, num) => list :+((num - 1) * binSize(n), num * binSize(n)))
        .toArray

    def imbalance(quote: Quote, array: Array[((Double, Double), Int)]): Int = {
      val imbalance = quote.bidSize / (quote.bidSize + quote.askSize)
      TransformRDD.binnedDouble(imbalance, array)._2
    }

    val listOfBins: Array[((Double, Double), Int)] = binList(numberOfBins).zipWithIndex


    val function: ((NumericTime, Quote)) => (Int, Double, Int, Int, Double, Int) = (q: (NumericTime, Quote)) =>
      (q._1.numericDate, q._1.numericTime,
        q._1.numericSecond,
        q._1.numericMillisecond,
        (q._2.ask + q._2.bid) / 2,
        imbalance(q._2, listOfBins))

    val dfA: RDD[MidImbalanceAux] =
      quoteSet.map(function).map(f => MidImbalanceAux(f._1, f._2, f._3, f._4, f._5, f._6))

    val fairDataFrame: RDD[(Int, FairDataFrame)] =
      dfA.map(v => v.date -> v).groupByKey()
        .flatMapValues( ls => {
          ls.map(x =>
            x -> (1 to numberOfLater).toArray
              .map(i => {
                  val filtered =
                    ls.filter(y =>  y.time <= x.time + dt._1 * i  )//&& y.time >= x.time + dt._1 * (i-1)

                   filtered.isEmpty match {
                    case false =>
                      val value = filtered.maxBy(_.time)
                      value.time -> Some(value.mid)
                    case true => x.time + dt._1 * i -> None
                  }
              })
          )
        })
        .map{ case(k,f) =>
          f._1.binImbalance ->
            FairDataFrame(f._1.date, f._1.time, f._1.second, f._1.milliSec, f._1.mid, f._1.binImbalance, f._2)
        }

    fairDataFrame


  }


  def conditionalExpectedValue(fairPriceDataFrame:  RDD[(Int, FairDataFrame)],numberOfLater: Int): RDD[(Int, Array[Double])] = {

    def combOp( iXs: (Int, Array[Double]), jYs: (Int,Array[Double])): (Int, Array[Double]) = {

      (iXs._1 + jYs._1) ->  iXs._2.zip(jYs._2).map(q => q._1 + q._2)
    }

    def seqOp(iXs: (Int, Array[Double]), y : FairDataFrame) : (Int,Array[Double]) = {

      val start: Array[Double] = y.laterMids.map{case(t, mid) =>
        mid match {
          case Some(m) => m - y.mid
          case None => 0.0
        }
      }

      (iXs._1 + 1) -> start.zip(iXs._2).map(q => q._1 + q._2)
    }

    fairPriceDataFrame.aggregateByKey(0 -> (1 to numberOfLater).toArray.map(v =>  0.0))(seqOp, combOp)
      .mapValues{case(numberOfQuotesInBin, processedVector) => processedVector.map(_/numberOfQuotesInBin)}

  }
}


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
    TransformRDD.binnedDouble(imbalance, array)._2
  }

}
