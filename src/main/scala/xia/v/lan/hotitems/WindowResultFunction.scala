package xia.v.lan.hotitems

import java.lang

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.functions.windowing.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * @author chenhao
  * @description <p>
  *              created by chenhao 2019/9/2 14:10
  */
class WindowResultFunction extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: lang.Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    val itemId = key.getField(0)
    val count = input.iterator().next()
    out.collect(new ItemViewCount(itemId,window.getEnd,count))
  }
}
