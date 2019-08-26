package com.hnjme.cloud.sql

import com.hnjme.cloud.sql.ValueData.ValueData
import com.hnjme.cloud.common.Utility.TimeUnit

/**
 * @author: heguangwu@163.com
 * @description: SQL abstract syntax tree
 * @date: 2019/08/22
 * @modify:
 */
object AST {
  sealed trait Node {
    /**
     * from AST transfer to SQL statement
     */
    def sql: String
  }
  trait SqlExpr extends Node
  trait BinaryOperator extends SqlExpr {
    val lhs: SqlExpr
    val rhs: SqlExpr
    val op: String

    override def sql: String = Seq(lhs.sql, op, rhs.sql) mkString " "
  }

  /**
   * arithmetic operators
   */
  case class Add(lhs: SqlExpr, rhs: SqlExpr, op: String = "+") extends BinaryOperator
  case class Subtract(lhs: SqlExpr, rhs: SqlExpr, op: String = "-") extends BinaryOperator
  case class Multiply(lhs: SqlExpr, rhs: SqlExpr, op: String = "*") extends BinaryOperator
  case class Divide(lhs: SqlExpr, rhs: SqlExpr, op: String = "/") extends BinaryOperator

  /**
   * SQL projects operators
   */
  trait SqlProject extends Node
  case class Projection(sqlProject: SqlProject, alias: Option[String]) extends Node {
    override def sql: String = Seq(Some(sqlProject.sql), alias).flatten.mkString(" AS ")
  }
  case class StarProject() extends SqlProject {
    override def sql: String = "*"
  }

  /**
   * SQL window time operator, like window(1h, 1m)
   */
  trait SqlTime extends Node
  case class WindowTime(time: Int, unit: TimeUnit) extends Node {
    override def sql: String = s"$time$unit"
  }
  case class HoppingWindow(window: WindowTime, hopping: WindowTime) extends SqlTime {
    override def sql: String = s"WINDOW($window, $hopping)"
  }

  /**
   * logical operators
   */
  case class Or(lhs: SqlExpr, rhs: SqlExpr) extends BinaryOperator {
    override val op: String = "OR"
  }
  case class And(lhs: SqlExpr, rhs: SqlExpr) extends BinaryOperator {
    override val op: String = "AND"
  }

  /**
   * comparison operators
   */
  trait EqualityLike extends BinaryOperator
  case class Eq(lhs: SqlExpr, rhs: SqlExpr) extends EqualityLike {
    override val op: String = "="
  }
  case class Neq(lhs: SqlExpr, rhs: SqlExpr) extends EqualityLike {
    override val op: String = "!="
  }

  case class Ge(lhs: SqlExpr, rhs: SqlExpr) extends EqualityLike {
    override val op: String = ">="
  }
  case class Gt(lhs: SqlExpr, rhs: SqlExpr) extends EqualityLike {
    override val op: String = ">"
  }
  case class Le(lhs: SqlExpr, rhs: SqlExpr) extends EqualityLike {
    override val op: String = "<="
  }
  case class Lt(lhs: SqlExpr, rhs: SqlExpr) extends EqualityLike {
    override val op: String = "<"
  }

  /**
   * aggregation operators
   */
  trait SqlAggregation extends Node
  case class Max(expr: SqlProject) extends SqlProject with SqlAggregation {
    override def sql: String = s"MAX(${expr.sql})"
  }
  case class Min(expr: SqlProject) extends SqlProject with SqlAggregation {
    override def sql: String = s"MIN(${expr.sql})"
  }
  case class Avg(expr: SqlProject, distinct: Boolean) extends SqlProject with SqlAggregation {
    override def sql: String = Seq(Some("AVG("), if (distinct) Some("DISTINCT ") else None,
      Some(expr.sql), Some(")")).flatten.mkString("")
  }
  case class Sum(expr: SqlProject, distinct: Boolean) extends SqlProject with SqlAggregation {
    override def sql: String = Seq(Some("SUM("), if (distinct) Some("DISTINCT ") else None,
      Some(expr.sql), Some(")")).flatten.mkString("")
  }
  case class CountExpr(expr: SqlProject, distinct: Boolean) extends SqlProject with SqlAggregation {
    override def sql: String = Seq(Some("COUNT("), if (distinct) Some("DISTINCT ") else None,
      Some(expr.sql), Some(")")).flatten.mkString("")
  }
  case class CountStar() extends SqlProject with SqlAggregation {
    override def sql: String = "COUNT(*)"
  }

  /**
   * table define, maybe it's more appropriate to call it topic
   */
  trait SqlRelation extends Node
  case class TableRelationAST(name: String, alias: Option[String]) extends SqlRelation {
    override def sql: String = Seq(Some(name), alias).flatten.mkString(" ")
  }

  /**
   * group by having clause not support
   */
  case class SqlGroupBy(keys: Seq[SqlProject], having: Option[SqlExpr] = None) extends Node {
    override def sql: String = Seq(Some("GROUP BY"), Some(keys.map(_.sql).mkString(", ")), having.map(e => "having " + e.sql)).flatten.mkString(" ")
  }


  /**
   * select statement, only support one table query
   */
  case class SelectStatement(projections: Seq[Projection],
                             relations: SqlRelation,
                             where: Option[SqlExpr],
                             groupBy: Option[SqlGroupBy],
                             window: Option[HoppingWindow]) extends Node {
    override def sql: String = Seq(Some("SELECT"), Some(projections.map(_.sql).mkString(", ")),
      Some("FROM " + relations.sql), where.map(x => "WHERE " + x.sql),
      groupBy.map(_.sql), window.map(_.sql)
    ).flatten.mkString(" ")
  }

  case object EmptyExpr extends SqlExpr {
    override def sql: String = ""
  }

  /**
   * literal definition package, like string, integer, double etc
   */
  case class Literal(value: ValueData[_]) extends SqlExpr with SqlProject {
    override def sql: String = value.toString
  }
  case class FieldIdent(qualifier: Option[String], name: String) extends SqlExpr with SqlProject {
    override def sql: String = Seq(qualifier, Some(name)).flatten.mkString(".")
  }
}
