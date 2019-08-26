package com.hnjme.cloud.sql

/**
 * @author: heguangwu@163.com
 * @description: SQL reserved keywords
 * @date: 2019/08/22
 * @modify:
 */
object KeyWords {
  /**
   * grammar keyword
   * use regular expression for case insensitive
   */
  val SELECT = "(?i)select".r
  val FROM = "(?i)from".r
  val OR = "(?i)or".r
  val AND = "(?i)and".r
  val AS = "(?i)as".r
  val WHERE = "(?i)where".r
  val DISTINCT = "(?i)distinct".r
  val GROUP = "(?i)group[\\s]+by".r

  //TODO not support
  val WINDOW = "(?i)window".r
  val NULL = "(?i)null".r

  /**
   * aggregate function
   */
  val MAX = "(?i)max".r
  val MIN = "(?i)min".r
  val AVG = "(?i)avg".r
  val SUM = "(?i)sum".r
  val COUNT = "(?i)count".r

  /**
   * functions
   */
  val DATE = "(?i)date".r

  /**
   * arithmetic operator
   */
  val ADD = "+"
  val SUBTRACT = "-"
  val MULTIPLY = "*"
  val DIVIDE = "/"
}
