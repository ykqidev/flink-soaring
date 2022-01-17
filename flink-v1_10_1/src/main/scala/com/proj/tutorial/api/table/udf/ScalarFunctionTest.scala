package com.proj.tutorial.api.table.udf

import com.proj.constant.FileConstants
import com.proj.tutorial.api.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Table}
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

object ScalarFunctionTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

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
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
      })

    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp.rowtime as 'ts)

    // 调用自定义hash函数，对id进行hash运算
    // 1. table api
    // 首先new一个UDF的实例
    val hashCode = new HashCode(23)
    val resultTable: Table = sensorTable
      .select('id, 'ts, hashCode('id))

    // 2. sql
    // 需要在环境中注册UDF
    tableEnv.createTemporaryView("sensor", sensorTable)
    tableEnv.registerFunction("hashCode", hashCode)
    val resultSqlTable: Table = tableEnv.sqlQuery("select id, ts, hashCode(id) from sensor")

    resultTable.toAppendStream[Row].print("result")
    resultSqlTable.toAppendStream[Row].print("sql")

    env.execute("scalar function test")
  }
}

// 自定义标量函数
class HashCode(factor: Int) extends ScalarFunction {
  def eval(s: String): Int = {
    s.hashCode * factor - 10000
  }
}
