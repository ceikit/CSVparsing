package MarketImpact

import java.io.File

import ParsingStructure.ModifiedNumericalKeyStamp
import org.apache.spark.sql.DataFrame

/**
  * Created by ceikit on 4/13/16.
  */
object WriteMinuteAggregateFiles {

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

    def writeFile(ds: DataFrame, fileName: String) =
      ds.coalesce(1).write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("minuteNumericAggregate/" + fileName.split('/')(6))

    import ModifiedNumericalKeyStamp.hiveContext.implicits._


    //val filesRDD: RDD[String] = SparkCSVParsing.sc.parallelize(filesList)

    filesList.foreach(d => writeFile(NumericMinuteAggregation(d).makeMinuteAggregateData().toDF(),d))


  }

  }
