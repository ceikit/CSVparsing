package MarketImpact

/**
  * Created by ceikit on 4/13/16.
  */
object ImpactMain {

  def main(args: Array[String]): Unit = {

    val tradesFile = "sashastoikov@gmail.com_5020.T_tradesQuotes_20130103_20150909.csv"

    //val hive = ModifiedNumericalKeyStamp.hiveContext

    lazy val minuteDataClass = NumericMinuteAggregation(tradesFile)

      lazy val minuteData = minuteDataClass.makeMinuteAggregateData()
      .sortBy(k => (k.day,k.hour))


     val volumeHist = minuteData.map(_.returns).histogram(100)
    //histogram(volumeHist._1.zip(volumeHist._2).toList)
    //line(minuteData.map(k => (k.day, k.hour, k.realizedVariance)).sortBy(v => (v._1,v._2, v._3)).collect().toList.map(_._3))

    //ModifiedNumericalKeyStamp.sc.stop()


  }

  }
