package com.hnjme.cloud.sql

import java.util.Date

import com.hnjme.cloud.sql.AST._
import com.hnjme.cloud.sql.KeyWords._
import scala.util.parsing.combinator.JavaTokenParsers

/**
 * @author: heguangwu@163.com
 * @description:
 * @date: 2019/08/23
 * @modify:
 */
//TODO StandardTokenParsers vs JavaTokenParsers
class SqlParser extends JavaTokenParsers{
  /**
   * reserved keywords ident
   */
  def reservedWords: Parser[String] = guard(SELECT | GROUP | WHERE | FROM | AS | DISTINCT | WINDOW |
    ADD | SUBTRACT | MULTIPLY | DIVIDE)
  def notReservedIdent: Parser[String] = not(reservedWords) ~> ident

  /**
   * BNF
   * literal::= numericLiteral | stringLiteral | floatLiteral | null | date
   */
  def literal: Parser[Literal] = wholeNumber ^^ (i => Literal(i.toInt)) |
    decimalNumber ^^ (i => Literal(i.toDouble)) |
    stringLiteral ^^ (s => Literal(s.toString)) |
    NULL ^^ (_ => Literal(null)) |
    DATE ~> stringLiteral ^^ { d => Literal(dateFormat(d)) }
  def exprLiteral: Parser[SqlExpr] = literal
  def selectLiteral: Parser[SqlProject] = literal

  /**
   * BNF
   * fieldIdent::= table.column | column
   */
  def fieldIdent: Parser[FieldIdent] = notReservedIdent ~ opt("." ~> notReservedIdent) ^^ {
    case table ~ Some(b: String) => FieldIdent(Option(table), b)
    case column ~ None => FieldIdent(None, column)
  }
  def exprIdent: Parser[SqlExpr] = fieldIdent
  def selectIdent: Parser[SqlProject] = fieldIdent

  def timeUnit = """[s|S|m|M|h|H|d|D]""".r
  def windowLiteral: Parser[WindowTime] =  wholeNumber ~ timeUnit ^^ {
    case t ~ u => WindowTime(t.toInt, u)
  }

  /**
   * BNF：
   * knowFunction ::= "count" "(" "*"| ["distinct"] singeSelect ")" | "min" "(" singeSelect ")" |
   *                  "max" "(" singeSelect ")" | "sum" "(" ["distinct"] singeSelect ")" |
   *                  "avg" "("  ["distinct"] singeSelect ")"
   */
  def knowFunction: Parser[SqlProject] = {
    def singeSelectExpr: Parser[SqlProject] = selectLiteral | selectIdent
    COUNT ~> "(" ~> ("*" ^^ (_ => CountStar()) |
      opt(DISTINCT) ~ singeSelectExpr ^^ { case d ~ e => CountExpr(e, d.isDefined) }) <~ ")" |
      MIN ~> "(" ~> singeSelectExpr <~ ")" ^^ { Min } |
      MAX ~> "(" ~> singeSelectExpr <~ ")" ^^ { Max } |
      SUM ~> "(" ~> (opt(DISTINCT) ~ singeSelectExpr) <~ ")" ^^ { case d ~ e => Sum(e, d.isDefined) } |
      AVG ~> "(" ~> (opt(DISTINCT) ~ singeSelectExpr) <~ ")" ^^ { case d ~ e => Avg(e, d.isDefined) }
  }

  /**
   * BNF
   * primarySelect ::= knowFunction | selectLiteral | selectIdent
   */
  def primarySelectExpr: Parser[SqlProject] = knowFunction | selectLiteral | selectIdent

  /**
   * BNF
   * projection ::= "*" | primarySelect ["as" ident]
   */
  def projection: Parser[Projection] = {
    "*" ^^ (_ => Projection(StarProject(), None)) |
      primarySelectExpr ~ opt(AS ~> ident) ^^ {
        case expr ~ alias => Projection(expr, alias)
      }
  }

  /**
   * BNF
   * projectStmt ::= projection {"," projection}
   */
  def projections: Parser[Seq[Projection]] = repsep(projection, ",")

  /**
   * BNF
   * fromStmt ::= "from" (ident ["as" indent] | { ident ["as" indent]})
   */
  //TODO current only support one table
  // def relations: Parser[Seq[SqlRelation]] = "from" ~> rep1sep(relation, ",")
  def relations: Parser[SqlRelation] = FROM ~> relation
  def relation: Parser[SqlRelation] = notReservedIdent ~ opt(AS) ~ opt(notReservedIdent) ^^ {
    case table ~ _ ~ alias => TableRelationAST(table, alias)
  }


  /**
   * BNF
   * primaryWhere ::= (literal | fieldIdent) ("=" | "<>" | "!=" | "<" | "<=" | ">" | ">=") (literal | fieldIdent)
   */
  def primaryWhereExpr: Parser[SqlExpr] = {
    (exprLiteral | exprIdent) ~ ("=" | "!=" | "<" | "<=" | ">" | ">=") ~ (exprLiteral | exprIdent) ^^ {
      case lhs ~ "=" ~ rhs => Eq(lhs, rhs)
      case lhs ~ "!=" ~ rhs => Neq(lhs, rhs)
      case lhs ~ "<" ~ rhs => Lt(lhs, rhs)
      case lhs ~ "<=" ~ rhs => Le(lhs, rhs)
      case lhs ~ ">" ~ rhs => Gt(lhs, rhs)
      case lhs ~ ">=" ~ rhs => Ge(lhs, rhs)
    } | "(" ~> expr <~ ")"
  }
  def andExpr: Parser[SqlExpr] = primaryWhereExpr * (AND ^^^ { (a: SqlExpr, b: SqlExpr) => And(a, b) })
  def orExpr: Parser[SqlExpr] = andExpr * (OR ^^^ { (a: SqlExpr, b: SqlExpr) => Or(a, b) })
  def expr: Parser[SqlExpr] = orExpr

  /**
   * BNF
   * whereStmt ::= "where" primaryWhere [("and"|"or") primaryWhere]
   */
  def whereStmt: Parser[SqlExpr] = WHERE ~> expr

  /**
   * BNF
   * groupbyStmt ::= "group by" fieldIdent {, fieldIdent } "group" ~> "by"
   */
//  def singeArithmeticExpr: Parser[SqlExpr] = opt("(") ~>
  def groupStmt: Parser[SqlGroupBy] = GROUP ~> rep1sep(selectIdent, ",") ^^ (keys => SqlGroupBy(keys))
  /**
   * BNF
   * windowStmt: ::= "window" ()
   */
  def windowStmt: Parser[HoppingWindow] = WINDOW ~> "(" ~> windowLiteral ~ "," ~ windowLiteral <~ ")" ^^ {
    case time ~ "," ~ interval => HoppingWindow(time, interval)
  }

  def select: Parser[SelectStatement] = {
    SELECT ~> projections ~ relations ~ opt(whereStmt) ~
      opt(groupStmt) ~ opt(windowStmt) <~ opt(";") ^^ {
      case p ~ r ~ f ~ g ~ w => SelectStatement(p, r, f, g, w)
    }
  }


  /**
   *
   * @param sql
   * @return
   */

  def parse(sql: String): Option[SelectStatement] = {
    parseAll(select, sql) match {
      case Success(r, _) => Some(r)
      case Failure(m, _) => //logo
        println(m)
        None
    }
  }

  def dateFormat(date: String, format: String = "yyyy-MM-dd HH:mm:ss"): Date = {
    val timeFormat = new java.text.SimpleDateFormat(format)
    timeFormat.parse(date)
  }
}

