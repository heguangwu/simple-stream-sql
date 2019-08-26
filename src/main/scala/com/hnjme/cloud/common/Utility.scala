package com.hnjme.cloud.common

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * @author: heguangwu@163.com
 * @description:
 * @date: 2019/08/24
 * @modify:
 */
object Utility {
  sealed trait TimeUnit
  case object Second extends TimeUnit {
    override def toString: String = "s"
  }
  case object Minute extends TimeUnit {
    override def toString: String = "m"
  }
  case object Hour extends TimeUnit {
    override def toString: String = "h"
  }
  case object Day extends TimeUnit {
    override def toString: String = "d"
  }

  implicit def string2TimeUnit(str: String): TimeUnit = str match {
    case "d" | "D" => Day
    case "h" | "H" => Hour
    case "m" | "M" => Minute
    case _ => Second
  }

  /**
   * create thread safe HashSet from Java
   */
  def createThreadSafeSet[T](): mutable.Set[T] = {
    java.util.Collections.newSetFromMap(new java.util.concurrent.ConcurrentHashMap[T, java.lang.Boolean]()).asScala
  }

}
