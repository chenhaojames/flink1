package api.day1

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
  * @author chenhao
  * @description <p>
  *              created by chenhao 2019/9/10 14:20
  */
object BatchWordCount {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = env.readTextFile("D:/temp/flink/全新安装.txt")
    val wordCount = text.flatMap(_.toLowerCase.split(" ").filter(_.nonEmpty))
      .map((_,1))
      .groupBy(0)
      .sum(1)
//    wordCount.print()
    wordCount.setParallelism(1).writeAsCsv("file:///D:/temp/flink/day1.csv","\n"," || ",WriteMode.OVERWRITE)
    //调用print方法时，会去执行env.execute方法
    env.execute("batch word count job")
  }
}
