import com.quantifind.charts.Highcharts._


case class WispDataPlotting(dataCheck : DataCheckSingleAsset) {

  lazy val tradesPerDay = dataCheck.tradeCheck.tradesPerDay

  lazy val volumePerDay = dataCheck.tradeCheck.volumePerDayTraded


  def tradesPerDayPlot = {
    val axisType: com.quantifind.charts.highcharts.AxisType.Type = "category"
    column(tradesPerDay.values.collect().toList).xAxis.map {
      axisArray => axisArray.map {
        _.copy(axisType = Option(axisType),
          categories = Option(tradesPerDay.keys.map(_.dateToString()).collect()))
      }
        legend(List("Number of trades per day"))
    }
  }

  def volumePerdayPlot = {
    column(volumePerDay.values.collect().toList); legend(List("Total Volume per day"))
  }

  def sizeHistogram(n: Int) = {
    //histogram(tradesCheck.tradesData.values.map(_.size).collect(), 50)
    histogram(dataCheck.sizeCheck.binnedSize(n).collect().toList)
    legend(List("Istogram of Trades Size"))

  }
}



  /*lazy val numberedColumns: Highchart = column(tradesPerDay.values.collect().toList)

  val axisType: com.quantifind.charts.highcharts.AxisType.Type = "category"

  val asseX: Option[Array[Axis]] = numberedColumns.xAxis.map {
    axisArray => axisArray.map { _.copy(axisType = Option(axisType),
      categories = Option(tradesPerDay.keys.map(_.dateToString()).collect())) }
  }*/

  //lazy val namedColumns: Highchart = numberedColumns.copy(xAxis = asseX)

  /*
  def alternativePlot = {
    
    for {trades <- tradesPerDay.values; i <- 1 to numberOfDays}{
      column
    }
    
  }*/

