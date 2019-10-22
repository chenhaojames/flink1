package api.day4

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka._

/**
  * @author chenhao
  * @description <p>
  *              created by chenhao 2019/10/21 10:25
  */
object FlinkKafka {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    // only required for Kafka 0.8
    properties.setProperty("zookeeper.connect", "localhost:2181")
    properties.setProperty("group.id", "test")
    val s: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("tes_topic", new SimpleStringSchema(), properties))
    s.print()

    /**
      * kafka连接时,报错: No entry found for connection
      *
      * 解决办法
      * ==================================
      * 在A主机的kafka配置文件($KAFKA_HOME/config/server.properties)中:
      * 添加:
      * advertised.listeners=PLAINTEXT://$IP_A:9092(默认这个key所在行是注释掉的)
      * 其中$IP_A可以是A主机的IP或者hostname(在B主机上能ping通就可以).
      * ==================================
      */
    env.execute("my flink kafka")
  }
}
