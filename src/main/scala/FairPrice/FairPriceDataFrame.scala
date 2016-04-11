package FairPrice

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import ParsingStructure._
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer

case class FairPriceDataFrame(quoteFile: String) {

  val hiveCtx = new HiveContext(ModifiedNumericalKeyStamp.sc)
  import hiveCtx.implicits._

  val quoteSet: RDD[(NumericTime, Quote)] =
    ModifiedNumericalKeyStamp.makeQuotesArray(quoteFile)
      .map{case(k,v)=> NumericTime (k.numericDate,k.numericTime,k.numericSecond, k.numericMillisecond) -> v}.persist()

  def makeMidPriceAux(numberOfBins : Int, dt: (Int,Int), numberOfLater: Int) = {

    def binSize(n: Int): Double = 1 / n.toDouble
    def binList(n: Int): Array[(Double, Double)] =
      (1 to n + 1)
        .foldLeft(ArrayBuffer[(Double, Double)]())((list, num) => list :+((num - 1) * binSize(n), num * binSize(n)))
        .toArray

    val listOfBins: Array[((Double, Double), Int)] = binList(numberOfBins).zipWithIndex


    val function = (q: (NumericTime, Quote)) =>
      MidPriceImbalance(q._1.numericDate,q._1.numericTime,
        q._1.numericSecond,
        q._1.numericMillisecond,
        (q._2.ask + q._2.bid) / 2,
        imbalance(q._2, listOfBins))

    val dfA: DataFrame = quoteSet.map(function).toDF()

    val dfC = dfA//.filter(dfA("second") === 46260 || dfA("second") === 46261 and dfA("date") === 41278)
    dfC.show()
    import org.apache.spark.sql.functions._

    val dfB = dfC.withColumnRenamed("second", "secondB")
      .withColumnRenamed("date", "dateB")
      .withColumnRenamed("milliSec", "milliSecB")
      .withColumnRenamed("mid", "midB")
      .withColumnRenamed("time", "timeB")

    dfB.show()


    dfC.join(
      dfB, dfB("dateB") === dfC("date") and
        dfB("secondB") === dfC("second") + dt._1  and
        dfB("timeB") - dfC("time") <=  dt._1 )
      .groupBy(dfC("date"), dfC("time"),dfC("second"),dfC("mid"),dfC("binImbalance"),dfB("timeB"), dfB("midB"))
      .agg(max(dfB("milliSecB")))


  }


  def imbalance(quote: Quote, array: Array[((Double, Double), Int)]) : Int = {
    val imbalance = quote.bidSize/(quote.bidSize + quote.askSize)
    TransformRDD.binnedDouble(10, imbalance, array)._2
  }


}