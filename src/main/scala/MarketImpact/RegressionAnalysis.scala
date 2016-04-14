package MarketImpact

import ParsingStructure.SparkCSVParsing
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.linalg
import org.apache.spark.mllib.regression.LinearRegressionWithSGD

/**
  * Created by ceikit on 4/13/16.
  */
object RegressionAnalysis {

  val sc = SparkCSVParsing.sc

  def regressionResult(fileName: String): RegressionResult = {

    val linearRegressionClass = LinearRegressionPlus(fileName)

    val splits = linearRegressionClass.tradeFlowFeature.randomSplit(Array(0.8,0.2))

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

    val weight: linalg.Vector = modelLinearRegression.weights

    RegressionResult(intercept, weight, rSquare)

  }
}

case class RegressionResult(intercept: Double, weight: linalg.Vector, rSquare: Double)
