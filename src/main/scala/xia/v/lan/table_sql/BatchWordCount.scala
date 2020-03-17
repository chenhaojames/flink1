package xia.v.lan.table_sql

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.table.descriptors.{FileSystem, OldCsv, Schema}

/**
  * @author chenhao
  * @description <p>
  *              created by chenhao 2020/3/16 15:11
  */
object BatchWordCount {

  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tbenv = BatchTableEnvironment.create(env)
    val path = BatchWordCount.getClass.getClassLoader.getResource("wd.txt").getPath
    tbenv.connect(new FileSystem().path(path))
      .withFormat(new OldCsv().field("word",Types.STRING).lineDelimiter("\n"))
      .withSchema(new Schema().field("word",Types.STRING))
      .registerTableSource("table1")
    val result: Table = tbenv.scan("table1")
      .groupBy("word")
      .select("word,count(1) as count")
    tbenv.toDataSet(result).print()

  }

}
