package com.proj.tutorial.api.sink

import java.util.Properties

import com.proj.tutorial.api.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}


object KafkaSinkTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 读取数据
    //    val inputPath = FileConstants.FILE_INPUT_PATH_TUTORIAL_API + "sensor.txt"
    //    val inputStream = env.readTextFile(inputPath)

    // 从kafka读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "localhost:9092")
    properties.setProperty("group.id", "consumer-group")
    val stream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))


    // 先转换成样例类类型（简单转换操作）
    val dataStream = stream
      .map(data => {
        val arr: Array[String] = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble).toString
      })

//    ??
//    dataStream.addSink(new FlinkKafkaProducer011[String]("localhost:9092", "sink", new SimpleStringSchema()))

    env.execute("kafka sink test")
  }
}
