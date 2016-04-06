

/**
 * Created by ceikit on 4/5/16.
 */
object FairPriceFiltering {

  def main(args: Array[String]): Unit = {

    val tradesFile ="sashastoikov@gmail.com_7203.T_tradesQuotes_20130103_20150909.csv.gz"
    val quoteFile ="sashastoikov@gmail.com_7203.T_events_20130103_20130706_frequency_-1.csv.gz"


    val dataSet = FairPriceDataFrame(quoteFile).makeMidPriceAux(10,(1,0),2)

    dataSet.show()
  }

}

case class A(time: Long, aValue: String)
case class B(time: Long, bValue: String)


//    val tableA = Seq(A(1, "q1"), A(2, "q2"), A(3, "q3"), A(4, "q4"), A(5, "q5"), A(6, "q6"), A(7, "q7"))
//    val tableB = Seq(B(2, "t1"), B(5, "t2"), B(7, "t3"))
//
//    val hiveCtx = new HiveContext(ModifiedNumericalKeyStamp.sc)
//
//    val dfA: DataFrame = hiveCtx.createDataFrame(tableA)
//    val dfB: DataFrame = hiveCtx.createDataFrame(tableB)
//
//    import org.apache.spark.sql.functions._
//
//    val one: DataFrame = dfA
//      .join(dfB, dfA("time") + 1 === dfB("time") and dfA("time") < dfB("time"))
//
//    one.show()
//
//    val two = one.groupBy(dfA("time"))
//      .agg(first(dfA("aValue")), min(dfB("time")))
//      .withColumnRenamed("FIRST(aValue)()", "aValue")
//      .withColumnRenamed("min(time)", "bTime")
//
//    two.show()
//
//      val prova = two.join(dfB, dfB("time") === two("bTime"))
//      .groupBy(dfB("time"))
//      .agg(collect_list(two("time")), collect_list(two("aValue")), first(dfB("time")), first(dfB("bValue")))
//
//
//    prova.show()
