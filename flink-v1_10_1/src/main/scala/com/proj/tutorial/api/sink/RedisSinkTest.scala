package com.proj.tutorial.api.sink

import com.proj.constant.FileConstants
import com.proj.tutorial.api.SensorReading
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object RedisSinkTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 读取数据
    val inputPath: String = FileConstants.FILE_INPUT_PATH_TUTORIAL_API + "sensor.txt";
    val inputStream: DataStream[String] = env.readTextFile(inputPath)

    // 先转换成样例类类型（简单转换操作）
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val arr: Array[String] = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })

    // 定义一个FlinkJedisConfigBase
    val conf: FlinkJedisPoolConfig = new FlinkJedisPoolConfig.Builder()
      .setHost("localhost")
      .setPort(6379)
      .build()

    dataStream.addSink(new RedisSink[SensorReading](conf, new MyRedisMapper))

    env.execute("redis sink test")
  }
}

// 定义一个RedisMapper
class MyRedisMapper extends RedisMapper[SensorReading] {
  // 定义保存数据写入redis的命令，HSET 表名 key value
  override def getCommandDescription: RedisCommandDescription = {
    new RedisCommandDescription(RedisCommand.HSET, "sensor_temp")
  }

  // 将温度值指定为value
  override def getValueFromData(data: SensorReading): String = data.temperature.toString

  // 将id指定为key
  override def getKeyFromData(data: SensorReading): String = data.id
}