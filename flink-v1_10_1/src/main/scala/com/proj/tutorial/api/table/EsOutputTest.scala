package com.proj.tutorial.api.table

import com.proj.constant.FileConstants
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala.StreamTableEnvironment
//import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors._


object EsOutputTest {
  def main(args: Array[String]): Unit = {
    // 1. 创建环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 2. 连接外部系统，读取数据，注册表
    val filePath: String = FileConstants.FILE_INPUT_PATH_TUTORIAL_API + "sensor.txt"

    tableEnv.connect(new FileSystem().path(filePath))
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("timestamp", DataTypes.BIGINT())
        .field("temp", DataTypes.DOUBLE())
      )
      .createTemporaryTable("inputTable")

    // 3. 转换操作
    val sensorTable: Table = tableEnv.from("inputTable")
    // 3.1 简单转换
    val resultTable: Table = sensorTable
      .select('id, 'temp)
      .filter('id === "sensor_1")

    // 3.2 聚合转换
    val aggTable: Table = sensorTable
      .groupBy('id) // 基于id分组
      .select('id, 'id.count as 'count)

    // 4. 输出到es
    tableEnv.connect(new Elasticsearch()
      .version("6")
      .host("localhost", 9200, "http")
      .index("sensor")
      .documentType("temperature")
    )
      .inUpsertMode()
      .withFormat(new Json())
      .withSchema(new Schema()
        .field("id", DataTypes.STRING())
        .field("count", DataTypes.BIGINT())
      )
      .createTemporaryTable("esOutputTable")

    aggTable.insertInto("esOutputTable")

    env.execute("es output test")

  }
}
