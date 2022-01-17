package com.proj.hot_items

import com.proj.constant.FileConstants
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Slide, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row


object HotItemsWithSql {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)


    val inputStream: DataStream[String] = env.readTextFile(FileConstants.FILE_INPUT_PATH_HOT_ITEMS + "user_behavior.csv")

    val dataStream: DataStream[UserBehavior] = inputStream
      .map(data => {
        val arr: Array[String] = data.split(",")
        UserBehavior(arr(0).toLong, arr(1).toLong, arr(2).toInt, arr(3), arr(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 定义表执行环境
    val settings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    // 基于DataStream创建Table
    val dataTable: Table = tableEnv.fromDataStream(dataStream, 'itemId, 'behavior, 'timestamp.rowtime as 'ts)

    // 1. Table API进行开窗聚合统计
    val aggTable: Table = dataTable
      .filter('behavior === "pv")
      .window(Slide over 1.hours every 5.minutes on 'ts as 'sw)
      .groupBy('itemId, 'sw)
      .select('itemId, 'sw.end as 'windowEnd, 'itemId.count as 'cnt)

    // 用SQL去实现TopN的选取
    tableEnv.createTemporaryView("aggtable", aggTable, 'itemId, 'windowEnd, 'cnt)
    val resultTable: Table = tableEnv.sqlQuery(
      """
        |select *
        |from (
        |  select
        |    *,
        |    row_number()
        |      over (partition by windowEnd order by cnt desc)
        |      as row_num
        |    from aggtable )
        |where row_num <= 5
      """.stripMargin)

    // 纯SQL实现
    tableEnv.createTemporaryView("datatable", dataStream, 'itemId, 'behavior, 'timestamp.rowtime as 'ts)
    val resultSqlTable: Table = tableEnv.sqlQuery(
      """
        |select *
        |from (
        |  select
        |    *,
        |    row_number()
        |      over (partition by windowEnd order by cnt desc)
        |      as row_num
        |    from (
        |      select
        |        itemId,
        |        hop_end(ts, interval '5' minute, interval '1' hour) as windowEnd,
        |        count(itemId) as cnt
        |      from datatable
        |      where behavior = 'pv'
        |      group by
        |        itemId,
        |        hop(ts, interval '5' minute, interval '1' hour)
        |    )
        |)
        |where row_num <= 5
      """.stripMargin)

    resultTable.toRetractStream[Row].print()

    env.execute("hot items sql job")
  }
}
