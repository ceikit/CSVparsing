package MarketImpact

/**
  * Created by ceikit on 4/8/16.
  */
case class AggregateDailyData(dateString: String,
                              dailyTradedVolume: Double,
                              dailyVolatility: Double,
                              dailyAverageSpread: Double)

case class AggregateNumericDailyData(day: Int,
                                   dailyTradedVolume: Double,
                                   dailyVolatility: Double,
                                   dailyAverageSpread: Double)

case class AggregateNumericDailyAux(day: Int,
                                    dailyTradedVolume: Double,
                                    dailyVolatility: Double,
                                    dailyAverageSpread: Double,
                                    cont: Int)