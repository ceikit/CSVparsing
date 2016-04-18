package MarketImpact

import java.io.File


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

    val exchangeFilesPath = "/Users/ceikit/Development/Scala/CSVparsing2/minuteNumericAggregate"

    val filesList: List[String] =
      getListOfSubDirectories(exchangeFilesPath)
        .flatMap(
          v => getListOfFiles(exchangeFilesPath + "/" + v)
          .map(s => exchangeFilesPath + "/" + v + "/" + s)
        )
        .filter(_.endsWith("0")).toList


    filesList.foreach(v => println(v))



    val regression = (v: String) => RegressionAnalysis.regressionResult(v)

    val filesRDD = RegressionAnalysis.sc.parallelize(filesList).zipWithIndex()

    import ParsingStructure.SparkCSVParsing.hiveContext.implicits._

    val regressionResult  = filesRDD.map(v => {println(v._2 + " " + v._1);regression(v._1)}).toDF()

     regressionResult.coalesce(1).write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("regressionResult.csv")

    regressionResult.show()




  }

  }
