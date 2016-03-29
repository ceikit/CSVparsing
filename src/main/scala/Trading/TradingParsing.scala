package Trading

import java.text.SimpleDateFormat

import com.quantifind.charts.Highcharts._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{Dataset, GroupedDataset}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by ceikit on 3/22/16.
 */
object TradingParsing {

  val conf = new SparkConf().setMaster("local[*]").setAppName("TradingMain")
  val sc = new SparkContext(conf)
  val hiveCtx = new HiveContext(sc)
  import Trading.TradingParsing.hiveCtx.implicits._


  def makeDataSet(fileName: String): Dataset[ClosePrice] = {
    sc.textFile(fileName)
      .map( t => {
      val temp = t.split(',')
      val tempHead = temp.head.split(' ')
      val date = tempHead(0).split('/')
      val year =  2000 + date(2).toInt
      val month = date(0).toInt
      val day = date(1).toInt

      val dateString =  s"$year-$month-$day"
      val date2 = new SimpleDateFormat("yyyy-M-d").parse(dateString)
      val date3 = new SimpleDateFormat("EE").format(date2)

      ClosePrice(dateString, date(1), date3, date(0), date(2), tempHead(1), temp.last.toDouble)
    } ).toDS()
      .filter( c => c.dayName != "Sun" && c.dayName != "Sat")// && c.hour < "20:00" && c.hour > "07:00" )
      .filter( c => c.dateString != "2015-12-25" && c.dateString != "2016-1-1" )
  }

  def times(fileName: String): GroupedDataset[String, ClosePrice] = {
    makeDataSet(fileName).filter( t => t.price == 35.62 || t.price == 39.97 || t.price == 38.1 || t.price == 37.04
      || t.price == 33.16 || t.price == 29.42 || t.price == 32.19).groupBy( _.day)
  }

  def plotPrice(fileName: String) = line(makeDataSet(fileName).map(_.price).collect().toList)

  def saveFile(fileName: String) = {
      makeDataSet(fileName).map { c =>
        val date = c.dateString
        val hour = c.hour
        val price = c.price
        s"$date, $hour, $price"
      }.toDF().coalesce(1)
        .write.text(fileName + "parsed")
  }


}
