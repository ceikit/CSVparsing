package FairPrice

import java.io.File

import ParsingStructure.{Quote, TransformRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.collection.immutable.IndexedSeq
;
;

/**
  * Created by ceikit on 4/28/16.
  */
object IteratedApproximation {

  def getListOfFiles(dir: String):List[String] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList.map(_.getName)
    } else {
      List[String]()
    }
  }

  def main(args: Array[String]): Unit = {

    val fileName = "nomuraMids"

    val sqlContext = ModifiedNumericalKeyStampFair.hiveContext
    val laterMidsFile  =
      fileName + '/' +
        getListOfFiles(fileName).filter(_.endsWith("0")).head

    val df: DataFrame = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(laterMidsFile).select("date","time","mid","binImbalance", "laterMids")

    def parseLaterColumn(s: String): Vector[Later] = s.replaceAll("[A-z]","").replace("(","").replace(")","")
      .split(',').map(_.toDouble).sliding(3,3).toVector.map(v => Later(v(0),v(1).toInt,v(2)))

    val dataSet2: RDD[(Int, FairDataFrame)] =
      df
      .map(row =>
        FairDataFrame(
          row(0).asInstanceOf[Int],
          row(1).asInstanceOf[Double],
          row(2).asInstanceOf[Double],
          row(3).asInstanceOf[Int],
          parseLaterColumn(row(4).asInstanceOf[String])
        )
      )
      .filter(v => v.date == 41374 )
      .map(v => v.binImbalance -> v)

    val symmetric: RDD[(Int, FairDataFrame)] = dataSet2.map{
      case(i, f) => 99 - i ->
        f.copy(
          mid = -f.mid,
          laterMids = f.laterMids.map(l => l.copy(imbalanceBin = 99 - l.imbalanceBin, mid = -l.mid))
        )
    }

    val dataSet: RDD[(Int, FairDataFrame)] = dataSet2.union(symmetric).cache()


//    val quoteFile =  "sashastoikov@gmail.com_8604.T_events_20130103_20130706_frequency_-1.csv.gz"
//    val classUsed = FairPriceDataFrameEfficient(quoteFile)


    val delays = Vector(0.01, 0.05, 0.1, 1.0, 5.0, 10.0 )
    val numberOfLags = delays.length
    val numberOfFunctions = 1000
    val numberOfBins = 100

    val functions: IndexedSeq[((Int, Int) => Double, (Int, Int) => Double)] =
      FunctionApproximation.makeFunctions(dataSet, numberOfLags, numberOfFunctions)


    val g10: Vector[(Int, Vector[Double])] = (0 to (numberOfBins - 1)).toVector
      .map(i => i -> (0 to (numberOfLags-1)).toVector
        .map(d => functions.last._2(i,d)))


    g10.foreach(println)

//    val firstPrice: Vector[Vector[Double]] =
//      classUsed.quoteSet
//        .map( q => q._1 -> aux(q._2,numberOfBins)).filter(v => v._2._2 >= 8 || v._2._2 <= 1 ).take(1000)
//        .map(q => q._1 ->  (q._2._1 +: functions.map(q._2._1 + _._2(q._2._2,0)).toVector))
//        .sortBy(_._1).map(_._2).toVector
//
//    firstPrice.foreach(println)
//    println(firstPrice.size)
//    println(functions.size)
//
//    import com.quantifind.charts.Highcharts._
//
//
//    val mid = firstPrice.map( k => k(0))
//    val oneDt = firstPrice.map( k => k(1))
//    val threeDt =  firstPrice.map( k => k(2))
//    val fourDt =  firstPrice.map( k => k(3))
//    val five =  firstPrice.map( k => k(4))
//    val six =  firstPrice.map( k => k(5))
//    val seven =  firstPrice.map( k => k(6))
//    val eight =  firstPrice.map( k => k(7))
//    val nine =  firstPrice.map( k => k(8))
//    val ten =  firstPrice.map( k => k(9))
//    line(mid)
//    hold
//    line(oneDt)
//    hold()
//    line(threeDt)
//    hold()
//    line(fourDt)
//    hold()
//    line(five)
//    hold()
//    line(six)
//    hold()
//    line(seven)
//    hold()
//    line(eight)
//    hold()
//    line(nine)
//    hold()
//    line(ten)
//    hold()
//    legend(List("mid","first", "second", "third", "fourth", "fifth", "sixth", "seventh", "eighth", "nineth", "tenth"))
//
//
//
//
//




  }

  def aux(q: Quote, numberOfBins: Int): (Double, Int) = {


    def imbalance(quote: Quote, array: Array[((Double, Double), Int)]): Int = {
      val imbalance = quote.bidSize / (quote.bidSize + quote.askSize)
      TransformRDD.binnedDouble(imbalance, array)._2
    }

    (q.ask + q.bid) / 2 -> imbalance(q, ImbalanceUtilities.listOfBins(10))
  }

}
