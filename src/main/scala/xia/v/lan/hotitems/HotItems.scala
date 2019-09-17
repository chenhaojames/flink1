package xia.v.lan.hotitems

import java.io.File

import myflink.HotItems.ItemViewCount
import org.apache.flink.api.common.io.FileInputFormat
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.{PojoCsvInputFormat, TextValueInputFormat}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.java.typeutils.PojoTypeInfo
import org.apache.flink.api.java.typeutils.TypeExtractor
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.{TimeCharacteristic, scala}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import xia.v.lan.hotitems.UserBehavior



/**
  * @author chenhao
  * @description <p>
  *              created by chenhao 2019/7/5 11:12
  */
object HotItems {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val fileUrl = HotItems.getClass.getClassLoader.getResource("hotItem.txt")
    val filePath: Path = new Path(fileUrl.toURI())
    val pojoType: PojoTypeInfo[UserBehavior] = TypeExtractor.createTypeInfo(classOf[UserBehavior]).asInstanceOf[PojoTypeInfo[UserBehavior]]
    val fieldOrder: Array[String] = Array[String]("userId", "itemId", "categoryId", "behavior", "timestamp")
    val csvInput = new PojoCsvInputFormat(filePath,pojoType,fieldOrder)
    val datasource: scala.DataStream[UserBehavior] = env.createInput(csvInput)(pojoType)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val timedData = datasource.assignTimestampsAndWatermarks(new AscendingTimestampExtractor[UserBehavior]() {
      override def extractAscendingTimestamp(element: UserBehavior): Long = element.timestamp.*(1000)
    })
    val longType: TypeInformation[Long] = TypeExtractor.createTypeInfo(classOf[Long])
    val stringType = TypeExtractor.createTypeInfo(classOf[String])
    val itemType = TypeExtractor.createTypeInfo(classOf[ItemViewCount])
    val pvData = timedData.filter(u => "pv".equalsIgnoreCase(u.behavior))
    val windowedData = pvData.keyBy("itemId")
        .timeWindow(Time.minutes(60),Time.minutes(5))
        .aggregate(new CountAgg(),new WindowFunction[Long, ItemViewCount, Tuple, TimeWindow]{
          override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
            val itemId = key.asInstanceOf[Tuple1[Long]].f0
            val count = input.iterator.next()
            out.collect(new ItemViewCount(itemId,window.getEnd,count))
          }
        })(longType,longType,itemType)

    val topItems = windowedData
      .keyBy("windowEnd")
        .process(new TopNHotItems(3))(stringType)
    topItems.print()
    env.execute("Flink Streaming Scala API Skeleton")
  }

  class AssignerWithPeriodicWatermarksDemo extends AssignerWithPeriodicWatermarks[Long,Int]{
    override def getCurrentWatermark: Watermark = ???

    override def extractTimestamp(t: Long, l: Long): Long = ???
  }

}


