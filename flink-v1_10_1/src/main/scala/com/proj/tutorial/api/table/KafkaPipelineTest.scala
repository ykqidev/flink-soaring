package com.proj.tutorial.api.table

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}

object KafkaPipelineTest {
  def main(args: Array[String]): Unit = {
    // 1. 创建环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 2. 从kafka读取数据
    tableEnv.connect(new Kafka()
      .version("0.11")
      .topic("sensor")
      .property("zookeeper.connect", "localhost:2181")
      .property("bootstrap.servers", "localhost:9092")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temperature", DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaInputTable")

    // 3. 查询转换
    // 3.1 简单转换
    val sensorTable: Table = tableEnv.from("kafkaInputTable")
    val resultTable: Table = sensorTable
      .select('id, 'temperature)
      .filter('id === "sensor_1")

    // 3.2 聚合转换
    val aggTable: Table = sensorTable
      .groupBy('id) // 基于id分组
      .select('id, 'id.count as 'count)

    // 4. 输出到kafka
    tableEnv.connect(new Kafka()
      .version("0.11")
      .topic("sinktest")
      .property("zookeeper.connect", "localhost:2181")
      .property("bootstrap.servers", "localhost:9092")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("temp", DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaOutputTable")

    resultTable.insertInto("kafkaOutputTable")

    env.execute("kafka pipeline test")
  }
}
