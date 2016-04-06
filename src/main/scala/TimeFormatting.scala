import java.time.LocalTime

import org.apache.poi.ss.usermodel.DateUtil

/**
 * Created by ceikit on 3/3/16.
 */

case class TQDateNoName(year: String, month: String, dayNumber: Int)

case class TQDate(year: String, month: String, dayNumber: Int, dayName: String ) extends Ordered[TQDate] {
  import scala.math.Ordered.orderingToOrdered
  def compare(that: TQDate) = (this.year, this.month, this.dayNumber) compare (that.year, that.month, that.dayNumber)
  def dateToString() = this.year + '/' + this.month + '/' + this.dayNumber + " (" + this.dayName + ')'
  def toNoName = TQDateNoName(year, month, dayNumber)
}

case class TQTimeStamp(time: String, milliseconds: Int) extends Ordered[TQTimeStamp]{
  import scala.math.Ordered.orderingToOrdered
  def compare(that: TQTimeStamp) = (this.time, this.milliseconds) compare (that.time, that.milliseconds)
}


case class TQTimeKey(date: TQDate, timeStamp: TQTimeStamp) extends Ordered[TQTimeKey]{
  import scala.math.Ordered.orderingToOrdered
  def compare(that: TQTimeKey): Int = (this.date, this.timeStamp) compare (that.date, that.timeStamp)
}

case class TQTimeKeyNumerical(numericDate: Int, numericTime: Double,numericSecond: Int, numericMillisecond: Int, date: TQDate, timeStamp: TQTimeStamp) extends Ordered[TQTimeKeyNumerical]{
  import scala.math.Ordered.orderingToOrdered
  def compare(that: TQTimeKeyNumerical) = (this.numericDate, this.numericTime) compare (that.numericDate, that.numericTime)
}

case class NumericTime(numericDate: Int, numericTime: Double, numericSecond: Int, numericMillisecond: Int) extends Ordered[NumericTime]{
  import scala.math.Ordered.orderingToOrdered
  def compare(that: NumericTime) = (this.numericDate, this.numericTime: Double) compare (that.numericDate, that.numericTime: Double)
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////

object TQTimeStamp {
  implicit def ordering[A <: TQTimeStamp]: Ordering[A] =
    Ordering.by(t => (t.time, t.milliseconds))
}

object TQDate{
  implicit def ordering[A <: TQDate]: Ordering[A] =
    Ordering.by(t => (t.year, t.month, t.dayNumber))
}

object TQTimeKey {
  implicit def ordering[A <: TQTimeKey]: Ordering[A] =
    Ordering.by(t => (t.date, t.timeStamp))
}

object TQTimeKeyNumerical {
  implicit def ordering[A <: TQTimeKeyNumerical]: Ordering[A] =
    Ordering.by(t => (t.numericDate, t.numericTime))
}

object NumericTime{
  implicit def ordering[A <: NumericTime]: Ordering[A] =
    Ordering.by(t => (t.numericDate, t.numericTime))
}


///////////////////////////////////////////////////////////////////////////////////////////////////////////////


object TimeFormattingUtilities {

  def fromCSVdateToTradeDate(dateT: String): TQDate = {
    val date: Array[String] = DateUtil.getJavaDate(dateT.toDouble).toString.split(' ')
    TQDate(date.last, parseMonth(date(1)), date(2).toInt, date(0))
  }

  def fromCSVtimeStampToTradeTimeStamp( timeStampT: String ) : TQTimeStamp = {
    val timeStamp = timeStampT.split('.')
    val time =  LocalTime.ofSecondOfDay(timeStamp(0).toLong).toString
    val milliSeconds = timeStamp(1)
    TQTimeStamp(time, milliSeconds.toInt)
  }

  def parseMonth(month: String) = {
    month match {
      case "Jan" => "01"
      case "Feb" => "02"
      case "Mar" => "03"
      case "Apr" => "04"
      case "May" => "05"
      case "Jun" => "06"
      case "Jul" => "07"
      case "Aug" => "08"
      case "Sep" => "09"
      case "Oct" => "10"
      case "Nov" => "11"
      case "Dec" => "12"
      case _     => sys.error(" NO MONTH matched")
    }
  }
}



