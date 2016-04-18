package MarketImpact

import java.io.File

import ParsingStructure.SparkCSVParsing
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

/**
  * Created by ceikit on 4/16/16.
  */
object MultipleStocksRegressionHistogram {

  def getListOfFiles(dir: String):List[String] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList.map(_.getName)
    } else {
      List[String]()
    }
  }

  def main(args: Array[String]): Unit = {

    val sc = SparkCSVParsing.sc
    val sqlContext = new SQLContext(sc)
    val regressionResultFile  =
      "regressionResult.csv" + '/' +
      getListOfFiles("regressionResult.csv").filter(_.endsWith("0")).head


    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .option("inferSchema", "true") // Automatically infer data types
      .load(regressionResultFile)

   // df.printSchema()
    //df.show()

    val regressionResultVector: RDD[RegressionResult] =
      df.map(row =>
        RegressionResult(
          row(0).asInstanceOf[Double],
          Vectors.dense(row(1).asInstanceOf[String].tail.dropRight(1).toDouble),
          row(2).asInstanceOf[Double])
      )

    import com.quantifind.charts.Highcharts._

    val interceptHistogram = regressionResultVector.map(_.intercept).histogram(100)
    //histogram(interceptHistogram._1.zip(interceptHistogram._2).toList)

    val weightHistogram = regressionResultVector.map(_.weight(0)).histogram(100)
    histogram(weightHistogram._1.zip(weightHistogram._2).toList)

    val rSquareHistogram = regressionResultVector.map(_.rSquare).histogram(100)
    histogram(rSquareHistogram._1.zip(rSquareHistogram._2).toList)



  }

  }
