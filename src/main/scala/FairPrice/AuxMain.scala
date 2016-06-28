package FairPrice

/**
  * Created by ceikit on 4/28/16.
  */
object AuxMain {

  def main(args: Array[String]): Unit = {

    val string = "WrappedArray([28801.874,[0,564.5]], [28801.874,[0,564.5]], [28802.873,[0,564.5]], [28808.877,[4,547.5]], [28898.66,[4,529.5]])"

    println(
      string
      .replaceAll("[A-z]","").replace("(","").replace(")","")
      .split(',').map(_.toDouble).sliding(3,3).toVector.map(v => Later(v(0),v(1).toInt,v(2))))


  }

  }
