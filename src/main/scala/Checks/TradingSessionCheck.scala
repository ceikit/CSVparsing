package Checks
import ParsingStructure._

import org.apache.spark.rdd.RDD

case class TradingSessionCheck(tradesData: RDD[(TQTimeKey, Trade)], quoteData: RDD[(TQTimeKey, Quote)]){

  lazy val tradesOffSession: RDD[(TQTimeKey, OffSession with Product with Serializable)] = offSession(tradesData)
  lazy val quotesOffSession: RDD[(TQTimeKey, OffSession with Product with Serializable)] = offSession(quoteData)

  lazy val summaryTradesOff: CounterWrapper = summary(tradesOffSession.values)
  lazy val summaryQuotesOff = summary(quotesOffSession.values)


  private type OffSessionPlus = OffSession with Product with Serializable

  private def summary(offSessionData : RDD[OffSessionPlus]): CounterWrapper = {
    val counter = offSessionData
      .aggregate(OffSessionCounter(0,0,0,0))(
        (count, value) => seqOp(count, value),
        (count1,count2) => combOp(count1,count2))

    offSessionData.first() match {
      case trade: TradesOffSession => CounterWrapper("TRADES", counter)
      case quote: QuotesOffSession => CounterWrapper("QUOTES", counter)
    }
  }


  private def offSession[T <: BookInfo](bookData: RDD[(TQTimeKey, T)])  = {
    bookData
      .map{ case (key, book) =>
      val timeStamp = key.timeStamp.time
      val beforeOpening = timeStamp < "09:00"
      val afterClosing = timeStamp > "15:00:00"
      val duringLunch = timeStamp > "11:30:00" && timeStamp < "12:30"
      book match {
        case trade: Trade => key -> TradesOffSession(trade, beforeOpening, afterClosing, duringLunch)
        case quote: Quote => key -> QuotesOffSession(quote, beforeOpening, afterClosing, duringLunch)
      }
    }
      .filter( off => off._2.beforeOpening || off._2.afterClosing || off._2.duringLunch)
  }

  private def seqOp( offSessionCounter: OffSessionCounter, offSession: OffSession): OffSessionCounter = {
    val befOp = offSessionCounter.totBeforeOpening
    val aftCl = offSessionCounter.totAfterClosing
    val durLu = offSessionCounter.totDuringLunch
    val tot = offSessionCounter.tot
    (offSession.beforeOpening, offSession.afterClosing, offSession.duringLunch) match {
      case (true, _ , _ ) => OffSessionCounter(befOp + 1, aftCl, durLu, tot + 1)
      case (false, true, _) => OffSessionCounter(befOp, aftCl + 1, durLu, tot +1)
      case (false, false, true) => OffSessionCounter(befOp, aftCl, durLu + 1, tot +1)
    }
  }

  private def combOp(count1: OffSessionCounter, count2: OffSessionCounter): OffSessionCounter ={
    OffSessionCounter(count1.totBeforeOpening + count2.totBeforeOpening,
      count1.totAfterClosing + count2.totAfterClosing,
      count1.totDuringLunch + count2.totDuringLunch,
      count1.tot + count2.tot)
  }


}

case class CounterWrapper(tradesOrQuotes: String, offSessionCounter: OffSessionCounter){

  def printSummary() = {
    println(tradesOrQuotes + " off-session" )
    println("----------------------")
    println("before market opens: " + offSessionCounter.totBeforeOpening )
    println("after market closes: " + offSessionCounter.totAfterClosing )
    println("during lunch break: " + offSessionCounter.totDuringLunch)
    println("----------------------")
    println("total: " + offSessionCounter.tot + '\n')
  }
}

case class OffSessionCounter(totBeforeOpening: Int, totAfterClosing: Int, totDuringLunch: Int, tot: Int)


abstract class OffSession{val beforeOpening: Boolean; val afterClosing: Boolean; val duringLunch: Boolean }

case class TradesOffSession(trade: Trade, beforeOpening: Boolean, afterClosing: Boolean, duringLunch: Boolean )
  extends OffSession

case class QuotesOffSession(trade: Quote, beforeOpening: Boolean, afterClosing: Boolean, duringLunch: Boolean )
  extends OffSession

