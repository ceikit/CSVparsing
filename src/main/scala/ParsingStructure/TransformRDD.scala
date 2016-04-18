package ParsingStructure

import org.apache.spark.rdd.RDD

/**
 * Created by ceikit on 3/29/16.
 */
object TransformRDD {

  def binnedRDD(n: Int, data: RDD[Double]): RDD[(String, Int)] ={
    val listOfBins: List[(Double, Double)] = binList(n)

    data.map( s => {

      val selectedBin: List[(Double, Double)] =
        listOfBins.filter( p => p._1 <= s && s <= p._2)

      selectedBin.head -> s
    } ).groupByKey().sortByKey()
      .map{case(key, value)  => (key._1.toFloat.toString + '-' + key._2.toFloat.toString, value.toList.length)}
      .filter{case(key, value) => value > 0}
  }

  def binnedDouble( x: Double, array: Array[((Double, Double), Int)]) : (String, Int) = {
    val selectedBin =
      array.filter( p => p._1._1 <= x && x <= p._1._2)
    val headT = selectedBin.head

    headT.toString() -> headT._2
  }

  def binnedRDD2(n: Int, data: RDD[Double]): RDD[(String, Int)] ={
    val listOfBins: List[(Double, Double)] = binList(n)
    data.map( s => {

      val selectedBin: List[(Double, Double)] =
        listOfBins.filter( p => p._1 <= s && s <= p._2)

      if( selectedBin.isEmpty) {println(s)}

      selectedBin.head -> s
    } ).groupByKey().sortByKey()
      .map{case(key, value)  => (key._1.toFloat.toString + '-' + key._2.toFloat.toString, value.toList.length)}
      .filter{case(key, value) => value > 0}
  }

  private def binSize(n: Int): Double = 1/n.toDouble
  private def binList(n: Int) =
    (1 to n+1).foldLeft( List[(Double,Double)]() )((list, num) => list :+ ((num-1) * binSize(n), num * binSize(n)) )

}
