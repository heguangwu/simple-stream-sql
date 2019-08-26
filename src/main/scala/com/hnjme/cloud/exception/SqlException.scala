package com.hnjme.cloud.exception

/**
 * @author: heguangwu@163.com
 * @description:
 * @date: 2019/08/23
 * @modify:
 */
case class DataTypeNotSupportException(msg: String) extends Exception(msg)
case class SqlParseException(msg: String) extends Exception(msg)
case class SqlQueryException(msg: String) extends Exception(msg)
