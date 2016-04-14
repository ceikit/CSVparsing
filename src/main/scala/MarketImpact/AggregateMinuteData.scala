package MarketImpact

/**
  * Created by ceikit on 4/8/16.
  */
case class AggregateMinuteData(dateString: String,
                               hour: String,
                               tradedVolume: Double,
                               tradeFlow: Double,
                               averageSpread: Double,
                               realizedVariance: Double,
                               returns: Double)


case class AggregateNumericalMinuteData(day: Int,
                                        hour: String,
                                        tradedVolume: Double,
                                        tradeFlow: Double,
                                        averageSpread: Double,
                                        realizedVariance: Double,
                                        returns: Double)

case class AggregateNumericalMinuteAux(day: Int,
                                        tradedVolume: Double,
                                        averageSpread: Double,
                                        realizedVariance: Double
                                      )