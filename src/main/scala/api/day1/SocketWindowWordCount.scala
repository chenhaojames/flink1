package api.day1

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * @author chenhao
  * @description <p>
  *              created by chenhao 2019/9/10 10:52
  */
object SocketWindowWordCount {

  def main(args: Array[String]): Unit = {
    val port = {
      try{
        ParameterTool.fromArgs(args).getInt("port")
      }catch {
        case e: Exception => {
          System.err.println("please input the socket port")
          return
        }
      }
    }
    /**
      * val port = 9000
      * cd D:\softwave\netcat\netcat-1.11
      * nc -l -p 9000
      */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost",port,'\n')
    val wordAndCount = text.flatMap(_.split("\\s"))
      .map(WordAndCount(_,1))
      .keyBy("word")
      .timeWindow(Time.seconds(5),Time.seconds(3))
      .sum("count")
    wordAndCount.print().setParallelism(1)
    env.execute("socket stream word and count job")
  }

  case class WordAndCount(word:String,count:Int)
}
