package com.proj.tutorial.api.table

import com.proj.constant.FileConstants
import com.proj.tutorial.api.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._


object Example {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 读取数据
    val inputPath: String = FileConstants.FILE_INPUT_PATH_TUTORIAL_API + "sensor.txt"
    val inputStream: DataStream[String] = env.readTextFile(inputPath)
    //    val inputStream = env.socketTextStream("localhost", 7777)

    // 先转换成样例类类型（简单转换操作）
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val arr: Array[String] = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })

    // 首先创建表执行环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    // 基于流创建一张表
    val dataTable: Table = tableEnv.fromDataStream(dataStream)

    // 调用table api进行转换
    val resultTable: Table = dataTable
      .select("id, temperature")
      .filter("id == 'sensor_1'")

    // 直接用sql实现
    tableEnv.createTemporaryView("dataTable", dataTable)
    val sql: String = "select id, temperature from dataTable where id = 'sensor_1'"
    val resultSqlTable: Table = tableEnv.sqlQuery(sql)

    resultTable.toAppendStream[(String, Double)].print("result")
    resultSqlTable.toAppendStream[(String, Double)].print("result sql")

    env.execute("table api example")
  }
}
