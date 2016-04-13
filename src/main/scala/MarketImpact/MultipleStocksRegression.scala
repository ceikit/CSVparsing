package MarketImpact

import java.io.File

import org.apache.spark.rdd.RDD


object MultipleStocksRegression {

  def getListOfFiles(dir: String):List[String] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList.map(_.getName)
    } else {
      List[String]()
    }
  }

  def getListOfSubDirectories(directoryName: String): Array[String] = {
     new File(directoryName).listFiles.filter(_.isDirectory).map(_.getName)
  }

  def main(args: Array[String]): Unit = {

    val exchangeFilesPath = "/Users/ceikit/sda/download/NIKKEI"

    val filesList: List[String] =
      getListOfSubDirectories(exchangeFilesPath)
        .flatMap(
          v => getListOfSubDirectories(exchangeFilesPath + "/" + v)
          .map(s => exchangeFilesPath + "/" + v + "/" + s)
        ).dropRight(1).toList
        .flatMap(f => getListOfFiles(f)
        .filter(_.endsWith(".gz")).map(v => f + "/" + v)
      )

    filesList.foreach(v => println(v))

    val regression = (v: String) => RegressionAnalysis.regressionResult(v)

    val filesRDD: RDD[String] = RegressionAnalysis.sc.parallelize(filesList)

    import ParsingStructure.SparkCSVParsing.hiveContext.implicits._

    val regressionResult  = filesRDD.map(v => regression(v)).toDF()

     regressionResult.coalesce(1).write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("regressionResult.csv")

    regressionResult.show()




  }

  }
