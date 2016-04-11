import Checks.DataCheckSingleAsset


object Main {

  def main(args: Array[String]): Unit ={

    val tradesFile = "sashastoikov@gmail.com_5020.T_tradesQuotes_20130103_20150909.csv"
    val quoteFile = "sashastoikov@gmail.com_5020.T_events_20130101_20131206_frequency_-1.csv.gz"

    lazy val dataCheck = DataCheckSingleAsset(tradesFile, quoteFile)




    //hold
    //line(dataCheck.quoteData.mapValues(q => ((q.bid+q.ask)/2-491.5)/(499.55-491.5)).sortByKey().values.take(3000).toList.drop(1000))

    //println(ManageDirectory.getListOfFiles( new File("/Users/ceikit/Development/Scala")))


  }

}