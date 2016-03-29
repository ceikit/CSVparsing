package Trading

import org.apache.spark.sql.Dataset


/**
 * Created by ceikit on 3/22/16.
 */
object TradingMain {

  def main(args: Array[String]): Unit ={


    val CL1File = "10minuteCL1.csv"

    val CL4File = "10minuteCL3.csv"


    val input: Dataset[ClosePrice] =  TradingParsing.makeDataSet(CL1File)

    import com.quantifind.charts.Highcharts._

    TradingParsing.plotPrice(CL1File)
    hold
    TradingParsing.plotPrice(CL4File)
    legend(List("CL1", "CL4"))
    val last = input.collect().last

    println(input.first(), last)

    lazy val times: Dataset[ClosePrice] = input.filter( t => t.price == 35.62 || t.price == 39.97 || t.price == 38.1 || t.price == 37.04
    || t.price == 33.16 || t.price == 29.42 || t.price == 32.19)


    //times.foreach( t => println(t))

    //TradingParsing.saveFile(CL1File)
    //TradingParsing.saveFile(CL4File)










  }

}
