package com.proj.tutorial.api.sink

import com.proj.constant.FileConstants
import com.proj.tutorial.api.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._


object FileSinkTest {
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

    dataStream.print()
    //    dataStream.writeAsCsv(FileConstants.FILE_OUTPUT_PATH_TUTORIAL_API + "out.txt")
    dataStream.addSink(
      StreamingFileSink.forRowFormat(
        new Path(FileConstants.FILE_OUTPUT_PATH_TUTORIAL_API + "out1.txt"),
        new SimpleStringEncoder[SensorReading]()
      ).build()
    )

    env.execute("file sink test")
  }
}
