package MarketImpact

import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.mllib.regression.{LassoWithSGD, LinearRegressionWithSGD}

/**
  * Created by ceikit on 4/12/16.
  */
object RegressionPlus {

  def main(args: Array[String]): Unit = {

    val fileNameChinese =  "sashastoikov@gmail.com_9984.T_tradesQuotes_20130103_20150909.csv"

    val linearRegressionClass = LinearRegressionPlus(fileNameChinese)


    val splits = linearRegressionClass.tradeFlowFeature.randomSplit(Array(0.8,0.2))

    val trainingSet = splits(0).cache()
    val testSet = splits(1).cache()

    val numTraining = trainingSet.count()
    val numTest = testSet.count()


    println(s"Training: $numTraining, test: $numTest.")
    readLine()

    // Building the model
    val numIterations = 1000
    val stepSize = 0.00000001



    val linearRegression = new LinearRegressionWithSGD()
    linearRegression.setIntercept(true)
    val modelLinearRegression = linearRegression.run(trainingSet)

    val predictionLinear = modelLinearRegression.predict(testSet.map(_.features))
    val predictionAndLabelLinear = predictionLinear.zip(testSet.map(_.label))


    /////////////////////////////////////////////////////

    val lasso = new LassoWithSGD()
    lasso.setIntercept(true)

    val modelLasso = lasso.run(trainingSet)

    val predictionLasso = modelLasso.predict(testSet.map(_.features))
    val predictionAndLabelLasso = predictionLasso.zip(testSet.map(_.label))

    println(modelLinearRegression)

    println(modelLasso)

    ///////////////////////////////////////////////////////



    // Instantiate metrics object
    val metricsLasso = new RegressionMetrics(predictionAndLabelLasso)
    val metricsLinear = new RegressionMetrics(predictionAndLabelLinear)


    // Squared error
    println(s"MSE = ${metricsLinear.meanSquaredError}")
    println(s"RMSE = ${metricsLinear.rootMeanSquaredError}")

    // R-squared
    println(s"R-squared = ${metricsLinear.r2}")

    // Mean absolute error
    println(s"MAE = ${metricsLinear.meanAbsoluteError}")

    // Explained variance
    println(s"Explained variance = ${metricsLinear.explainedVariance}")


    // Squared error
    println(s"MSE = ${metricsLasso.meanSquaredError}")
    println(s"RMSE = ${metricsLasso.rootMeanSquaredError}")

    // R-squared
    println(s"R-squared = ${metricsLasso.r2}")

    // Mean absolute error
    println(s"MAE = ${metricsLasso.meanAbsoluteError}")

    // Explained variance
    println(s"Explained variance = ${metricsLasso.explainedVariance}")


  }
}
