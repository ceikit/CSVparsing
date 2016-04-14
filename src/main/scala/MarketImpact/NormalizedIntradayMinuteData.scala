package MarketImpact

/**
  * Created by ceikit on 4/8/16.
  */
case class NormalizedIntradayMinuteData(dateString: String,
                                        hour: String,
                                        tradedVolume: Double,
                                        tradeFlow: Double,
                                        averageSpread: Double,
                                        realizedVariance: Double,
                                        returns: Double,
                                        volumePercentage: Double,
                                        normalizedVolatility: Double,
                                        normalizedSpread: Double)
