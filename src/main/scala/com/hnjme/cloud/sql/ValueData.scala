package com.hnjme.cloud.sql

import java.util.Date

import com.hnjme.cloud.exception.DataTypeNotSupportException

/**
 * @author: heguangwu@163.com
 * @description:
 * @date: 2019/08/23
 * @modify:
 */
object ValueData {
  sealed trait ValueData[T] {
    def value: T

    override def toString: String = value.toString
    override def hashCode: Int = value.hashCode()
    override def equals(other: Any): Boolean = other match {
      case that: ValueData[T] => that.value == value
      case _ => false
    }
  }

  /**
   * support data type
   */
  implicit class IntValue(val value: Int) extends ValueData[Int]
  implicit class LongValue(val value: Long) extends ValueData[Long]
  implicit class DoubleValue(val value: Double) extends ValueData[Double]
  implicit class StringValue(val value: String) extends ValueData[String] {
    override def toString: String = s""""$value""""
  }
  implicit class DateValue(val value: Date) extends ValueData[Date] {
    //transform unix timestamp only for pass test
    override def toString: String = (value.getTime / 1000).toString
  }

  /**
   * literal compare auxiliary function
   * @param x
   * @param y
   * @return: see compareTo function
   */
  def compareHelper(x: ValueData[_], y: ValueData[_]): Int = (x, y) match {
    case (x: IntValue, y: IntValue) => x.value.compareTo(y.value)
    case (x: LongValue, y: LongValue) => x.value.compareTo(y.value)
    case (x: DoubleValue, y: DoubleValue) => x.value.compareTo(y.value)
    case (x: StringValue, y: StringValue) => x.value.compareTo(y.value)
    case (x: DateValue, y: DateValue) => x.value.compareTo(y.value)
    case (x: IntValue, y: LongValue) => x.value.toLong.compareTo(y.value)
    case (x: LongValue, y: IntValue) => x.value.compareTo(y.value)
    case (x: DoubleValue, y: IntValue) => x.value.compareTo(y.value)
    case (x: DoubleValue, y: LongValue) => x.value.compareTo(y.value)
    case _ => x.value.toString.compareTo(y.value.toString)
  }

  /**
   * use for aggregation compute
   */
  implicit val valueNumeric = new Numeric[ValueData[_]] {
    override def plus(x: ValueData[_], y: ValueData[_]): ValueData[_] = (x, y) match {
      case (x: IntValue, y: IntValue) => x.value + y.value
      case (x: IntValue, y: LongValue) => x.value + y.value
      case (x: IntValue, y: DoubleValue) => x.value + y.value
      case (x: LongValue, y: LongValue) => x.value + y.value
      case (x: LongValue, y: IntValue) => x.value + y.value
      case (x: LongValue, y: DoubleValue) => x.value + y.value
      case (x: DoubleValue, y: DoubleValue) => x.value + y.value
      case (x: DoubleValue, y: IntValue) => x.value + y.value
      case (x: DoubleValue, y: LongValue) => x.value + y.value
      case _ => throw DataTypeNotSupportException(s"add exception, x type: ${x.getClass}, y type: ${y.getClass}")
    }

    override def minus(x: ValueData[_], y: ValueData[_]): ValueData[_] = (x, y) match {
      case (x: IntValue, y: IntValue) => x.value - y.value
      case (x: IntValue, y: LongValue) => x.value - y.value
      case (x: IntValue, y: DoubleValue) => x.value - y.value
      case (x: LongValue, y: LongValue) => x.value - y.value
      case (x: LongValue, y: IntValue) => x.value - y.value
      case (x: LongValue, y: DoubleValue) => x.value - y.value
      case (x: DoubleValue, y: DoubleValue) => x.value - y.value
      case (x: DoubleValue, y: IntValue) => x.value - y.value
      case (x: DoubleValue, y: LongValue) => x.value - y.value
      case _ => throw DataTypeNotSupportException(s"minus exception, x type: ${x.getClass}, y type: ${y.getClass}")
    }

    override def times(x: ValueData[_], y: ValueData[_]): ValueData[_] = (x, y) match {
      case (x: IntValue, y: IntValue) => x.value * y.value
      case (x: IntValue, y: LongValue) => x.value * y.value
      case (x: IntValue, y: DoubleValue) => x.value * y.value
      case (x: LongValue, y: LongValue) => x.value * y.value
      case (x: LongValue, y: IntValue) => x.value * y.value
      case (x: LongValue, y: DoubleValue) => x.value * y.value
      case (x: DoubleValue, y: DoubleValue) => x.value * y.value
      case (x: DoubleValue, y: IntValue) => x.value * y.value
      case (x: DoubleValue, y: LongValue) => x.value * y.value
      case _ => throw DataTypeNotSupportException(s"times exception, x type: ${x.getClass}, y type: ${y.getClass}")
    }

    override def negate(x: ValueData[_]): ValueData[_] = x match {
      case x: IntValue => - x.value
      case x: LongValue => - x.value
      case x: DoubleValue => -x.value
      case _ => throw DataTypeNotSupportException(s"negate exception, data type: ${x.getClass}, value: $x")
    }

    override def fromInt(x: Int): ValueData[_] = x
    override def toInt(x: ValueData[_]): Int = x match {
      case x: IntValue => x.value
      case x: LongValue => x.value.toInt
      case x: DoubleValue => x.value.toInt
      case _ => throw DataTypeNotSupportException(s"toInt exception, data type: ${x.getClass}, value: $x")
    }

    override def toLong(x: ValueData[_]): Long = x match {
      case (x: IntValue) => x.value.toLong
      case (x: LongValue) => x.value
      case (x: DoubleValue) => x.value.toLong
      case _ => throw DataTypeNotSupportException(s"toLong exception, data type: ${x.getClass}, value: $x")
    }

    override def toFloat(x: ValueData[_]): Float = x match {
      case (x: IntValue) => x.value.toFloat
      case (x: LongValue) => x.value.toFloat
      case (x: DoubleValue) => x.value.toFloat
      case _ => throw DataTypeNotSupportException(s"toFloat exception, data type: ${x.getClass}, value: $x")
    }
    override def toDouble(x: ValueData[_]): Double = x match {
      case (x: IntValue) => x.value.toDouble
      case (x: LongValue) => x.value.toDouble
      case (x: DoubleValue) => x.value
      case _ => throw DataTypeNotSupportException(s"toDouble exception, data type: ${x.getClass}, value: $x")
    }

    override def compare(x: ValueData[_], y: ValueData[_]): Int = compareHelper(x, y)
  }
}
