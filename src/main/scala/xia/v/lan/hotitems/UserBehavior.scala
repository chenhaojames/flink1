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
  *              created by chenhao 2019/7/5 11:14
  */
class UserBehavior() {
  var userId: Long = 0
  var itemId:Long = 0
  var categoryId:Int = 0
  var behavior:String = ""
  var timestamp:Long = 0

  def this(userId: Long,itemId:Long,categoryId:Int,behavior:String,timestamp:Long){
    this()
    this.userId = userId
    this.itemId = itemId
    this.categoryId = categoryId
    this.behavior = behavior
    this.timestamp = timestamp
  }

}



object UserBehavior{

}
