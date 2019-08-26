package com.hnjme.cloud.query

import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue}


/**
 * @author: heguangwu@163.com
 * @description:
 * @date: 2019/08/25
 * @modify:
 */
trait TimeSerialData

case class SwitchTimeSerialData(timestamp: Long, value: Int) extends TimeSerialData
case class AnalogTimeSerialData(timestamp: Long, value: Double) extends TimeSerialData

object RealTimeData {
  val realTimeData = new ConcurrentHashMap[String, TimeSerialData]()
  val windowData = new ConcurrentHashMap[String, LinkedBlockingQueue[TimeSerialData]]()

  def update(key: String, value: TimeSerialData) = {
    realTimeData.put(key, value)
    val window = windowData.get(key)
    if(window != null) {
      window.add(value)
    }
  }
}
