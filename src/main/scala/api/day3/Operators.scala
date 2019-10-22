package api.day3

import org.apache.flink.streaming.api.scala._

/**
  * @author chenhao
  * @description <p>
  *              created by chenhao 2019/9/18 14:01
  */
object Operators {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val intList = env.fromElements(1,2,3,4,5)
    val strList = env.fromElements("a","b","c","d","e")
    val connectedStream = intList.connect(strList)
    val map1 = connectedStream.map(
      (_:Int) => true,
      (_:String) => false
    )
//    map1.print()
    val split: SplitStream[Int] = intList.split(
      /*(num: Int) =>
        (num % 2) match {
          case 0 => List("even")
          case 1 => List("odd")
        }*/
      matchcase(_)
    )

    val t1: DataStream[Array[String]] = strList.map(_.split(","))
    val t2: DataStream[String] = strList.flatMap(_.split(","))
    strList

    split.print()

    env.execute("day3")
  }

  def matchcase(num:Int) = num%2 match {
    case 0 => List("even")
    case 1 => List("odd")
  }
}
