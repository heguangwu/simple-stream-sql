package com.hnjme.cloud.query

import java.time.Instant

/**
 * @author: heguangwu@163.com
 * @description:
 * @date: 2019/08/25
 * @modify:
 */
trait Window {
  val startMs: Long
  val endMs: Long
  val startTime: Instant = Instant.ofEpochMilli(startMs)
  val endTime: Instant = Instant.ofEpochMilli(endMs)

  override def hashCode(): Int = (((startMs << 32) | endMs) % 0xFFFFFFFFL).toInt
  override def equals(obj: Any): Boolean = obj match {
    case other: Window => startMs == other.startMs && endMs == other.endMs
    case _ => false
  }

  override def toString: String = s"Window{startMs=$startMs, endMs=$endMs}"
}
