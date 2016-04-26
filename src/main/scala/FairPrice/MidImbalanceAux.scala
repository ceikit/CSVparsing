package FairPrice

/**
  * Created by ceikit on 4/11/16.
  */

case class MidImbalanceAux(date: Int, time: Double, second: Int, milliSec: Int, mid: Double, binImbalance: Int)

case class FairDataFrame(date: Int, time: Double, second: Int, milliSec: Int,
                         mid: Double, binImbalance: Int, laterMids: Array[(Double ,Option[(Int, Double)] )])

