package xia.v.lan.table_sql

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.sources.CsvTableSource
import org.apache.flink.types.Row

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

    /*val text = fsEnv.socketTextStream("localhost", 9000, '\n')
    val wordAndCount: DataStream[(String, Int)] = text.flatMap(_.split("\\s")).map((_,1))
    val t2: Table = fsTabEnv.fromDataStream(wordAndCount)
    val t3: Table = fsTabEnv.sqlQuery(
      """
        |select * from t2
      """.stripMargin)
    t3.printSchema()*/

    val tmp1: DataStream[(Int, String, String)] = fsEnv.fromElements((1,"a","aa"),(2,"b","bb"),(3,"c","cc"))
    val tmp2: DataStream[(Int, String, Int)] = fsEnv.fromElements((1,"x",2),(2,"y",2),(3,"z",1))
    fsTabEnv.registerDataStream("table1", tmp1,'id1,'name1,'flag1)
    fsTabEnv.registerDataStream("table2",tmp2,'id2,'pic_name2,'t1_id)
//    val result1 = fsTabEnv.sqlQuery(
//      """
//        |select * from table1
//      """.stripMargin)
//    val result2 = fsTabEnv.sqlQuery(
//      """
//        |select * from table2
//      """.stripMargin)
//    result1.leftOuterJoin(result2)
    val result4 = fsTabEnv.sqlQuery(
      """
        |select t1.*,t2.* from table1 t1 inner join table2 t2 on t1.id1 = t2.t1_id
      """.stripMargin)
    val ds1: DataStream[(Boolean, Row)] = fsTabEnv.toRetractStream[Row](result4)
    fsEnv.setParallelism(1)
    ds1.map(_._2).print()
    fsEnv.execute("table sql demo")
  }

}
