package MarketImpact

import java.io.File

import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionWithSGD}
import org.apache.spark.rdd.RDD

/**
  * Created by ceikit on 4/16/16.
  */
object ExchangeAggregateRegression {

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

    val filesRDD: RDD[AggregateNumericalMinuteData] =
      filesList.map(k => LinearRegressionPlus(k).minuteAggregateData.rdd)
        .reduce((d1,d2) =>  d1.union(d2))

    val tradeFlowFeature: RDD[LabeledPoint] = filesRDD.map(
          row => LabeledPoint(row.returns/math.sqrt(row.realizedVariance) ,
            Vectors.dense(row.tradeFlow/row.tradedVolume))
        ).persist()

    val splits = tradeFlowFeature.randomSplit(Array(0.8,0.2))

    val trainingSet = splits(0).cache()
    val testSet = splits(1).cache()

    val linearRegression = new LinearRegressionWithSGD()
    linearRegression.setIntercept(true)
    val modelLinearRegression = linearRegression.run(trainingSet)

    val predictionLinear = modelLinearRegression.predict(testSet.map(_.features))
    val predictionAndLabelLinear = predictionLinear.zip(testSet.map(_.label))

    val metricsLinear = new RegressionMetrics(predictionAndLabelLinear)

    val rSquare = metricsLinear.r2

    val intercept = modelLinearRegression.intercept

    val weight = modelLinearRegression.weights

    println(rSquare, intercept, weight)





  }

}
