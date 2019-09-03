package xia.v.lan.hotitems

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

/**
  * @author chenhao
  * @description <p>
  *              created by chenhao 2019/9/2 14:23
  */
class TopNHotItems(var topSize: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String] {

  var itemState : ListState[ItemViewCount] = null

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    val itemsStateDesc = new ListStateDescriptor("itemState-state", classOf[ItemViewCount])
    itemState  = getRuntimeContext().getListState(itemsStateDesc)
  }

  override def onTimer(timestamp: Long, ctx: _root_.org.apache.flink.streaming.api.functions.KeyedProcessFunction[_root_.org.apache.flink.api.java.tuple.Tuple, _root_.xia.v.lan.hotitems.ItemViewCount, _root_.scala.Predef.String]#OnTimerContext, out: _root_.org.apache.flink.util.Collector[_root_.scala.Predef.String]): Unit = {
    var allItems = List.empty[ItemViewCount]
    val itr = itemState.get().iterator()
    while(itr.hasNext){
      allItems = allItems.+:(itr.next())
    }
    itemState.clear()
    allItems = allItems.sortWith((a,b) =>  a.viewCount - b.viewCount > 0 )
    val result = new StringBuffer();
    result.append("====================================\n")
    result.append("时间: ").append(timestamp - 1).append("\n")
    for(i <- 0 until topSize){
      val currentItem = allItems(i)
      result.append("No").append(i).append(":")
        .append("  商品ID=").append(currentItem.itemId).append("  浏览量=")
        .append(currentItem.viewCount).append("\n")
    }
    result.append("====================================\n")
    // 控制输出频率，模拟实时滚动结果
    Thread.sleep(1000)
    out.collect(result.toString)
  }

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    itemState.add(value)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }
}
