package com.proj.tutorial.api.sink

import java.util

import com.proj.constant.FileConstants
import com.proj.tutorial.api.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests


object EsSinkTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 读取数据
    val inputPath: String = FileConstants.FILE_INPUT_PATH_TUTORIAL_API + "sensor.txt"
    val inputStream: DataStream[String] = env.readTextFile(inputPath)

    // 先转换成样例类类型（简单转换操作）
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val arr: Array[String] = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })

    // 定义HttpHosts
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("localhost", 9200))

    // 自定义写入es的EsSinkFunction
    val myEsSinkFunc: ElasticsearchSinkFunction[SensorReading] = new ElasticsearchSinkFunction[SensorReading] {
      override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        // 包装一个Map作为data source
        val dataSource = new util.HashMap[String, String]()
        dataSource.put("id", t.id)
        dataSource.put("temperature", t.temperature.toString)
        dataSource.put("ts", t.timestamp.toString)

        // 创建index request，用于发送http请求
        val indexRequest: IndexRequest = Requests.indexRequest()
          .index("sensor")
          .`type`("readingdata")
          .source(dataSource)

        // 用indexer发送请求
        requestIndexer.add(indexRequest)

      }
    }

    dataStream.addSink(new ElasticsearchSink
      .Builder[SensorReading](httpHosts, myEsSinkFunc)
      .build()
    )

    env.execute("es sink test")
  }
}
