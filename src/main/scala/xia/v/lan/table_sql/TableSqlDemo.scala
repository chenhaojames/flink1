package xia.v.lan.table_sql

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.sources.CsvTableSource

/**
  * @author chenhao
  * @description <p>
  *              created by chenhao 2019/9/9 15:01
  */
object TableSqlDemo {

  def main(args: Array[String]): Unit = {
    val fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
    val fsEnv = StreamExecutionEnvironment.getExecutionEnvironment
    val fsTabEnv = StreamTableEnvironment.create(fsEnv,fsSettings)

    val text = fsEnv.socketTextStream("localhost", 9000, '\n')
    val wordAndCount: DataStream[(String, Int)] = text.flatMap(_.split("\\s")).map((_,1))
    val t2: Table = fsTabEnv.fromDataStream(wordAndCount)
    val t3: Table = fsTabEnv.sqlQuery(
      """
        |select * from t1
      """.stripMargin)
  }

}
