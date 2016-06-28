package Example

import java.io.File

import FairPrice.{FairDataFrame, Later, ModifiedNumericalKeyStampFair}
import org.apache.spark.sql.DataFrame

/**
  * Created by ceikit on 5/17/16.
  */
object ImbalancePath {

  def getListOfFiles(dir: String): List[String] = {
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
    val laterMidsFile =
      fileName + '/' +
        getListOfFiles(fileName).filter(_.endsWith("0")).head

    val df: DataFrame = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(laterMidsFile).select("date", "time", "mid", "binImbalance", "laterMids")

    def parseLaterColumn(s: String): Vector[Later] = s.replaceAll("[A-z]", "").replace("(", "").replace(")", "")
      .split(',').map(_.toDouble).sliding(3, 3).toVector.map(v => Later(v(0), v(1).toInt, v(2)))

    val dataSet2 =
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
        .filter(v => v.date == 41374).sortBy(_.time)
        .map(v => v.binImbalance -> v.mid).take(10000).drop(3000)


    import com.quantifind.charts.Highcharts._

    line(dataSet2.map(_._1).toVector)
    hold
    line(dataSet2.map(_._2).toVector)




  }
}
