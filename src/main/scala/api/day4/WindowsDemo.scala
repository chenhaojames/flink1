package api.day4

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @author chenhao
  * @description <p>
  *              created by chenhao 2019/10/21 10:04
  */
object WindowsDemo {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val list = env.fromElements("a","c","a","c","1")
    val wc = list.flatMap(r => r).map((_,1)).keyBy(0).timeWindow(Time.seconds(5)).sum(1)
    wc.print()
    env.execute("hello 3q")
  }
}
