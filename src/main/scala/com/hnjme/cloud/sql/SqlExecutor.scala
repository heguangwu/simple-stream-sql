package com.hnjme.cloud.sql

import com.hnjme.cloud.exception.{DataTypeNotSupportException, SqlParseException, SqlQueryException}
import com.hnjme.cloud.sql.AST._
import com.hnjme.cloud.sql.ValueData._


/**
 * @author: heguangwu@163.com
 * @description: SQL execute compute
 * @date: 2019/08/23
 * @modify:
 */
object SqlExecutor {
  /**
   * row define, table define
   */
  type LogicalRow = Map[String, ValueData[_]]
  type LogicalTable = List[LogicalRow]
  /**
   * table schema define, table name
   */
  type LogicalSchema = Map[String, LogicalTable]

  def LogicalRow(elems: (String, ValueData[_])*) = Map(elems: _*)
  def LogicalTable(xs: LogicalRow*) = List(xs: _*)

  /**
   * execute entrance
   * @param table: query table data
   * @param sql: sql lexical parse tree
   * @return
   */
  def execute(table: LogicalTable, sql: SelectStatement): LogicalTable = {
    sql match {
      case SelectStatement(s, f, w, g, t) => t match {
        case Some(windowQuery) =>
          // TODO 查询 window表
          table from f where w sqlGroupBy g sqlAggregate s project s
        case None => table from f where w sqlGroupBy g sqlAggregate s project s
      }
    }
  }

  implicit class Relation2Table(table: LogicalTable) {
    def from(relations: SqlRelation) = table

    def where(where: Option[SqlExpr]): LogicalTable = {
      where match {
        case None => table
        case Some(exp: SqlExpr) => table filter (whereEachRow(_, exp))
      }
    }

    /**
     * the function is not be name groupBy，because table is List, name groupBy will call List.groupBy function
     * @param g
     * @return
     */
    def sqlGroupBy(g: Option[SqlGroupBy]): Seq[LogicalTable] = {
      g match {
        case Some(exp: SqlGroupBy) => group(table, exp)
        case None => Seq(table)
      }
    }
  }

  /**
   * 这个隐式转换主要是适配group by的结果和后续要求类型不匹配的问题
   * sqlGroupBy的结果为Seq[Table]而后续的执行如order by等需要的是Table
   * 另外，projection中的聚合函数如SUM等操作也在这里执行
   */
  implicit class GroupByMidTable(tables: Seq[LogicalTable]) {
    def sqlAggregate(projections: Seq[Projection]): Seq[LogicalTable] = {
      if (projections exists (haveAggregate(_))) {
        tables.map(execAggregates(_, projections))
      } else {
        tables
      }
    }

    def haveAggregate(projection: Projection): Boolean = projection.sqlProject match {
      case _: SqlAggregation => true
      case _ => false
    }

    def project(projections: Seq[Projection]): LogicalTable = {
      tables.map(execProject(_, projections)) reduce (_ ++ _)
    }
  }

  def whereEachRow(row: LogicalRow, expr: SqlExpr): Boolean = {
    def equalityLike(expr: EqualityLike, f: (ValueData[_], ValueData[_]) => Boolean) = {
      (expr.lhs, expr.rhs) match {
        case (left: Literal, right: Literal) => f(left.value, right.value)
        case (left: FieldIdent, right: Literal) => row.get(left.name) match {
          case Some(s) => f(s, right.value)
          case None => false
        }
        case (left: FieldIdent, right: FieldIdent) => f(row(left.name), row(right.name))
        case _ => throw SqlParseException(s"$expr not supported")
      }
    }
    def eq(a: ValueData[_], b: ValueData[_]): Boolean = a == b
    def neq(a: Any, b: Any): Boolean = a != b

    def lt(a: ValueData[_], b: ValueData[_])(implicit ev1: ValueData[_] => Ordered[ValueData[_]]): Boolean = a < b
    def le(a: ValueData[_], b: ValueData[_])(implicit ev1: ValueData[_] => Ordered[ValueData[_]]): Boolean = a <= b
    def gt(a: ValueData[_], b: ValueData[_])(implicit ev1: ValueData[_] => Ordered[ValueData[_]]): Boolean = a > b
    def ge(a: ValueData[_], b: ValueData[_])(implicit ev1: ValueData[_] => Ordered[ValueData[_]]): Boolean = a >= b

    expr match {
      case And(left, right) => whereEachRow(row, left) && whereEachRow(row, right)
      case Or(left, right) => whereEachRow(row, left) || whereEachRow(row, right)
      case (expr: Eq) => equalityLike(expr, eq)
      case (expr: Neq) => equalityLike(expr, neq)
      case (expr: Lt) => equalityLike(expr, lt)
      case (expr: Gt) => equalityLike(expr, gt)
      case (expr: Le) => equalityLike(expr, le)
      case (expr: Ge) => equalityLike(expr, ge)
    }
  }

  /**
   * group by返回的结果是Seq[Table]，这里的Table其实并不是表的所有数据
   * 而是根据group by的field筛选出来的原表数据的一小部分，可以理解为子表
   * 所以这里需要将传入的子表的数据调用一系列的聚合函数后结果为一行Row数据
   * 这里有一个约束，如果没有聚合函数情况下，仅返回子表结果的第一行数据
   */
  def execAggregates(table: LogicalTable, expr: Seq[Projection]): LogicalTable = {
    val result = expr map {
      case Projection(es, alias) => es match {
        case e: SqlAggregation => alias match { //执行聚合函数，根据是否有别名来替换最终结果Key
          case None => execAggregate(table, e)
          case Some(newName: String) => LogicalRow(newName -> execAggregate(table, e).map(_._2).head)
        }
        case e: FieldIdent => table.head collect {
          case (e.name, _2) => (e.name, _2)
        }
        case _: StarProject => table.head
      }
    }
    LogicalTable(result.flatten.toMap)
  }

  /**
   * aggregate function call
   * @param table
   * @param function
   * @return
   */
  def execAggregate(table: LogicalTable, function: SqlAggregation): LogicalRow = {
    function match {
      case Max(e: FieldIdent) => {
        val tuple = table.maxBy(row => row(e.name)).collect {
          case (e.name, value) => ("max(" + e.name + ")", value)
        }.head
        Map(tuple._1 -> tuple._2)
      }
      case Min(e: FieldIdent) => {
        val tuple = table.minBy(row => row(e.name)).collect {
          case (e.name, value) => ("min(" + e.name + ")", value)
        }.head
        Map(tuple._1 -> tuple._2)
      }
      case Max(e: Literal) => Map("max(" + e.value + ")" -> e.value)
      case Min(e: Literal) => Map("min(" + e.value + ")" -> e.value)

      case CountStar() => Map("count(*)" -> table.size)
      case CountExpr(e: FieldIdent, distinct: Boolean) => {
        if (distinct) {
          val countDist = (table map (row => row(e.name)) distinct).size
          Map("count(distinct " + e.name + ")" -> countDist)
        } else {
          Map("count(distinct " + e.name + ")" -> table.size)
        }
      }
      case CountExpr(e: Literal, distinct: Boolean) => {
        if (distinct) {
          Map("count(distinct" + e.value + ")" -> 1)
        } else {
          Map("count(distinct" + e.value + ")" -> table.size)
        }
      }
      case Sum(e: FieldIdent, distinct: Boolean) => {
        if (distinct) {
          val sum = (table map (row => row(e.name))).toSet.sum
          Map("sum(distinct " + e.name + ")" -> sum)
        } else {
          val sum = (table map (row => row(e.name))).sum
          Map("sum(" + e.name + ")" -> sum)
        }
      }
      case Sum(e: Literal, distinct: Boolean) => {
        if (distinct) {
          Map("sum(distinct " + e.value + ")" -> 1)
        } else {
          e.value match {
            case x: IntValue => Map("sum(" + e + ")" -> x.value * table.size)
            case x: LongValue => Map("sum(" + x + ")" -> x.value * table.size)
            case x: DoubleValue => Map("sum(" + x + ")" -> x.value * table.size)
            case _ => throw DataTypeNotSupportException(s"data type ${e.value.getClass} can not support")
          }
        }
      }
      case Avg(e: Literal, distinct: Boolean) => {
        if (distinct) {
          Map("avg(distinct " + e.value + ")" -> e.value)
        } else {
          Map("avg(" + e.value + ")" -> e.value)
        }
      }
      case Avg(e: FieldIdent, distinct: Boolean) => {
        if (distinct) {
          val sum = (table map (row => row(e.name))).toSet.sum
          sum match {
            case s: DoubleValue => Map("avg( distinct" + e.name + ")" -> s.value / table.size)
            case s: IntValue => Map("avg( distinct" + e.name + ")" -> s.value / table.size)
            case s: LongValue => Map("avg(" + e.name + ")" ->  s.value / table.size)
            case _ => throw DataTypeNotSupportException(s"data type ${sum.getClass} can not support")
          }
        } else {
          val sum = (table map (row => row(e.name))).sum
          sum match {
            case s: DoubleValue => Map("avg(" + e.name + ")" -> s.value / table.size)
            case s: IntValue => Map("avg(" + e.name + ")" -> s.value.toDouble / table.size)
            case s: LongValue => Map("avg(" + e.name + ")" -> s.value.toDouble / table.size)
            case _ => throw DataTypeNotSupportException(s"data type ${sum.getClass} can not support")
          }
        }
      }
    }
  }

  /**
   * select execute auxiliary function
   * @param table
   * @param expr
   * @return
   */
  def execProject(table: LogicalTable, expr: Seq[Projection]): LogicalTable = {
    table map (projectEachRow(_, expr))
  }
  def projectEachRow(row: LogicalRow, expr: Seq[Projection]): LogicalRow = {
    expr map (e => row collect (satisfy(e))) reduce(_ ++ _)
  }

  /**
   * 这个偏函数用于根据expr的类型返回不同的函数,如果为“*”则原样返回，如果有别名就替换别名
   * 偏函数的入参和出参类型都是(String, ValueData[_]), 偏函数的返回函数原型为：
   *    def f(in: (String, ValueData[_])): (String, ValueData[_])
   */
  def satisfy(expr: Projection): PartialFunction[(String, ValueData[_]), (String, ValueData[_])] = {
    (expr.sqlProject, expr.alias) match {
      case (StarProject(), _) => {
        case (filed, value) => (filed, value)  //查询参数为 * 直接返回
      }
      case (f: FieldIdent, alias) => alias match {
        case None => {
          case (f.name, value) => (f.name, value)
        }
        case Some(alias: String) => {
          case (f.name, value) => (alias, value)
        }
      }
      case (l: Literal, alias) => alias match {
        case None => {
          case (_, _) => (l.value.toString, l.value)
        }
        case Some(alias: String) => {
          case (_, _) => (alias, l.value)
        }
      }
      case (_: SqlAggregation, _) => {
        case (filed, value) => (filed, value)
      }
      case _ => throw SqlParseException(s"lexical not support ${expr} project ")
    }
  }

  /**
   * groupBy execute auxiliary function
   * @param t
   * @param groupKeys
   * @return
   */
  //groupBy辅助执行类
  def group(t: LogicalTable, groupKeys: SqlGroupBy): Seq[LogicalTable] = {
    val keys: Seq[String] = groupKeys.keys map {
      case x: FieldIdent => x.name
      case _ => throw new SqlQueryException("group by must have FieldIdent")
    }
    //group result: Map(List(groupKey) -> Row)
    t.groupBy(row => keys.map(row(_))).map(_._2).toSeq
  }
}
