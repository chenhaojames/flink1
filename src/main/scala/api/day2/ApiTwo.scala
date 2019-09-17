package api.day2

import java.util

import org.apache.flink.streaming.api.datastream.DataStreamUtils
import org.apache.flink.streaming.api.scala._

/**
  * @author chenhao
  * @description <p>
  *              created by chenhao 2019/9/16 10:23
  */
object ApiTwo {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val someIntegers: DataStream[Long] = env.generateSequence(0, 1000)
    val iteratedStream: DataStream[Long] = someIntegers.iterate(
      iteration => {
        val minusOne: DataStream[Long] = iteration.map(v => v - 10)
        val stillGreaterThanZero: DataStream[Long] = minusOne.filter (_ > 0)
        val lessThanZero: DataStream[Long] = minusOne.filter(_ <= 0)
        (stillGreaterThanZero, lessThanZero)
      }
    )
//    iteratedStream.print()
    val result = DataStreamUtils.collect(iteratedStream.javaStream)
    result
    val e1 = env.fromElements(1,2,3)

    env.execute("")
  }

}
