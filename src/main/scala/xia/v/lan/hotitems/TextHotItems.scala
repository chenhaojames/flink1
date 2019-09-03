package xia.v.lan.hotitems

import java.lang

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.{PojoCsvInputFormat, TextInputFormat, TextValueInputFormat}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.api.java.typeutils.{PojoTypeInfo, TypeExtractor}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor
import org.apache.flink.streaming.api.{TimeCharacteristic, scala}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
  * @author chenhao
  * @description <p>
  *              created by chenhao 2019/9/3 10:55
  */
object TextHotItems {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val fileUrl = HotItems.getClass.getClassLoader.getResource("hotItem.txt")
    val filePath: Path = new Path(fileUrl.toURI())
    val pojoType = TypeExtractor.createTypeInfo(classOf[UserBehavior]).asInstanceOf[PojoTypeInfo[UserBehavior]]
    val txtTypeInfomation = TypeExtractor.createTypeInfo(classOf[String])
    val userTypeInfomation = TypeExtractor.createTypeInfo(classOf[UserBehavior])
    val txtInput = new TextInputFormat(filePath)
    val txtDatasource = env.createInput(txtInput)(txtTypeInfomation)
    val datasource: DataStream[UserBehavior] = txtDatasource.map(r => {
      val arr = r.split(",")
      val userBehavior = new UserBehavior(
        arr(0).toLong,
        arr(1).toLong,
        arr(2).toInt,
        arr(3),
        arr(4).toLong)
      userBehavior
    })(userTypeInfomation)
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
}
