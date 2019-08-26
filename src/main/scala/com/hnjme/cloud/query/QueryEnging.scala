package com.hnjme.cloud.query

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import com.hnjme.cloud.sql.SqlParser

/**
 * @author: heguangwu@163.com
 * @description:
 * @date: 2019/08/24
 * @modify:
 */
object QueryEnging {
  /**
   * sql query columns and reference count
   */
  val querykeys = new ConcurrentHashMap[String, AtomicInteger]()

  val parser = new SqlParser

  def addStreamQuery(sql: String) = {

  }

  def main(args: Array[String]): Unit = {// group by sex where sex > 0 or age < 9
    parser.parse("Select MAX(age), sex AS A from user As u WHERE sex > 0 And age < 9 OR sex < 0 Group by sex Window(5M, 10s)") match {
      case Some(r) => {
        println(s"1111:${r.projections}")
        println(s"2222:${r.relations}")
        println(s"3333:${r.where}")
        println(s"4444:${r.groupBy}")
        println(s"5555:${r.window}")
      }
      case None => println("==============")
    }
  }
}
