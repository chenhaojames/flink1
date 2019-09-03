package xia.v.lan.hotitems

import java.lang

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * @author chenhao
  * @description <p>
  *              created by chenhao 2019/9/2 11:04
  */
class CountAgg extends AggregateFunction[UserBehavior,Long, Long] {
  override def createAccumulator(): Long = 0

  override def add(in: UserBehavior, acc: Long): Long = acc+1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc1 + acc
}

object CountAgg{
  def main(args: Array[String]): Unit = {
    var list = List.apply(1,2,4,1,8,9,6)
    list = list.sortWith((a,b) => a - b < 0)
    list.foreach(System.err.println(_))
  }
}






