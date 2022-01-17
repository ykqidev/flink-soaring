package com.proj.login_fail

import java.net.URL
import java.util

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object LoginFailAdvance {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 读取数据
    val resource: URL = getClass.getResource("/login_fail/login_log.csv")
    val inputStream: DataStream[String] = env.readTextFile(resource.getPath)

    // 转换成样例类类型，并提起时间戳和watermark
    val loginEventStream: DataStream[LoginEvent] = inputStream
      .map(data => {
        val arr: Array[String] = data.split(",")
        LoginEvent(arr(0).toLong, arr(1), arr(2), arr(3).toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(3)) {
        override def extractTimestamp(element: LoginEvent): Long = element.timestamp * 1000L
      })

    // 进行判断和检测，如果2秒之内连续登录失败，输出报警信息
    val loginFailWarningStream: DataStream[LoginFailWarning] = loginEventStream
      .keyBy(_.userId)
      .process(new LoginFailWaringAdvanceResult())

    loginFailWarningStream.print()
    env.execute("login fail detect job")
  }
}

class LoginFailWaringAdvanceResult() extends KeyedProcessFunction[Long, LoginEvent, LoginFailWarning] {
  // 定义状态，保存当前所有的登录失败事件，保存定时器的时间戳
  lazy val loginFailListState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginfail-list", classOf[LoginEvent]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginFailWarning]#Context, out: Collector[LoginFailWarning]): Unit = {
    // 首先判断事件类型
    if (value.eventType == "fail") {
      // 1. 如果是失败，进一步做判断
      val iter: util.Iterator[LoginEvent] = loginFailListState.get().iterator()
      // 判断之前是否有登录失败事件
      if (iter.hasNext) {
        // 1.1 如果有，那么判断两次失败的时间差
        val firstFailEvent: LoginEvent = iter.next()
        if (value.timestamp < firstFailEvent.timestamp + 2) {
          // 如果在2秒之内，输出报警
          out.collect(LoginFailWarning(value.userId, firstFailEvent.timestamp, value.timestamp, "login fail 2 times in 2s"))
        }
        // 不管报不报警，当前都已处理完毕，将状态更新为最近依次登录失败的事件
        loginFailListState.clear()
        loginFailListState.add(value)
      } else {
        // 1.2 如果没有，直接把当前事件添加到ListState中
        loginFailListState.add(value)
      }
    } else {
      // 2. 如果是成功，直接清空状态
      loginFailListState.clear()
    }
  }
}