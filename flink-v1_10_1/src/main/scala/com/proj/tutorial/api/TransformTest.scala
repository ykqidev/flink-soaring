package com.proj.tutorial.api

import com.proj.constant.FileConstants
import org.apache.flink.api.common.functions.{FilterFunction, MapFunction, ReduceFunction, RichMapFunction}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

object TransformTest {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 0.读取数据
    val inputPath: String = FileConstants.FILE_INPUT_PATH_TUTORIAL_API + "sensor.txt"
    val inputStream: DataStream[String] = env.readTextFile(inputPath)

    // 1.先转换成样例类类型（简单转换操作）
    val dataStream: DataStream[SensorReading] = inputStream
      .map(data => {
        val arr: Array[String] = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      })
    //          .filter(new MyFilter)

    // 2.分组聚合，输出每个传感器当前最小值
    val aggStream: DataStream[SensorReading] = dataStream
      .keyBy("id") // 根据id进行分组
      .minBy("temperature")

    //    aggStream.print()


    // 3.需要输出当前最小的温度值，以及最近的时间戳，要用reduce
    val resultStream: DataStream[SensorReading] = dataStream
      .keyBy("id")
      //      .reduce( (curState, newData) =>
      //        SensorReading( curState.id, newData.timestamp, curState.temperature.min(newData.temperature) )
      //      )
      .reduce(new MyReduceFunction)

    //        resultStream.print()


    // 4. 多流转换操作
    // 4.1 分流，将传感器温度数据分成低温、高温两条流
    val splitStream: SplitStream[SensorReading] = dataStream
      .split(data => {
        if (data.temperature > 30.0) Seq("high") else Seq("low")
      })
    val highTempStream: DataStream[SensorReading] = splitStream.select("high")
    val lowTempStream: DataStream[SensorReading] = splitStream.select("low")
    val allTempStream: DataStream[SensorReading] = splitStream.select("high", "low")

//        highTempStream.print("high")
//        lowTempStream.print("low")
//        allTempStream.print("all")

    // 4.2 合流，connect
    val warningStream: DataStream[(String, Double)] = highTempStream.map(data => (data.id, data.temperature))
//    warningStream.print()
    val connectedStreams: ConnectedStreams[(String, Double), SensorReading] = warningStream.connect(lowTempStream)
//    lowTempStream.print()
    // 用coMap对数据进行分别处理
    val coMapResultStream: DataStream[Any] = connectedStreams
      .map(
        waringData => (waringData._1, waringData._2, "warning"),
        lowTempData => (lowTempData.id, "healthy")
      )
    coMapResultStream.print()

    // 4.3 union合流
    val unionStream: DataStream[SensorReading] = highTempStream.union(lowTempStream, allTempStream)

    unionStream.print("coMap")

    env.execute("transform test")
  }
}

class MyReduceFunction extends ReduceFunction[SensorReading] {
  override def reduce(value1: SensorReading, value2: SensorReading): SensorReading =
    SensorReading(value1.id, value2.timestamp, value1.temperature.min(value2.temperature))
}

// 自定义一个函数类
class MyFilter extends FilterFunction[SensorReading] {
  override def filter(value: SensorReading): Boolean =
    value.id.startsWith("sensor_1")
}

class MyMapper extends MapFunction[SensorReading, String] {
  override def map(value: SensorReading): String = value.id + " temperature"
}

// 富函数，可以获取到运行时上下文，还有一些生命周期
class MyRichMapper extends RichMapFunction[SensorReading, String] {

  override def open(parameters: Configuration): Unit = {
    // 做一些初始化操作，比如数据库的连接
    //    getRuntimeContext
  }

  override def map(value: SensorReading): String = value.id + " temperature"

  override def close(): Unit = {
    //  一般做收尾工作，比如关闭连接，或者清空状态
  }
}