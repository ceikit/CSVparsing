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
