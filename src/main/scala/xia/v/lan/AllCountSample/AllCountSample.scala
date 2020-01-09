package xia.v.lan.AllCountSample

import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import scala.util.Random

/**
  * @author chenhao
  * @description <p>
  *              created by chenhao 2020/1/9 15:11
  */
object AllCountSample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    env.addSource(new DataSource).keyBy(0).sum(1)
      .keyBy(t2 => "")
      .timeWindow(Time.seconds(15),Time.seconds(5))
      .fold(Map.empty[String,Int])((a1,a2) => {
        a1.+(a2.f0 -> a2.f1)
      })
      .addSink(ms => {
        println(ms)
        println(ms.values.sum)
      })
    env.execute("AllCountSample")
  }
}

class DataSource extends RichParallelSourceFunction[Tuple2[String, Int]]{
  var flag : Boolean = true

  override def run(ctx: SourceFunction.SourceContext[Tuple2[String, Int]]): Unit = {
    while(flag){
      Thread.sleep((getRuntimeContext.getIndexOfThisSubtask + 1) * 1000 * 5)
      val key = "类别" + Random.nextInt(3)
      val value = Random.nextInt(10) + 1
      println("Emits\t", key, value)
      ctx.collect(Tuple2.of(key,value))
    }
  }

  override def cancel(): Unit = {
    flag = false
  }
}
