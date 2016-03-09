import com.quantifind.charts.Highcharts._

/**
 * Created by ceikit on 3/8/16.
 */


case class WispDataPlotting(tradesCheck : TradeCheck) {

  lazy val tradesPerDay = tradesCheck.tradesPerDay

  lazy val volumePerDay = tradesCheck.volumePerDayTraded


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

