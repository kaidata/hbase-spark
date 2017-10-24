package org.apache.spark.sql.catalyst.util

import scala.util.matching.Regex
import scala.util.parsing.combinator.lexical._
import scala.util.parsing.input.CharArrayReader.EofCh

/**
  * Description: 
  *
  * Project：hbase-spark
  * Department：海康研究院
  * User: likai14
  * Date: 2017-10-24
  * Time: 10:13
  * Email: likai14@hikvision.com / likai14@hikvision.com.cn
  * Copyright©2017 Rights Reserved 杭州海康威视数字技术股份有限公司 版权所有 浙ICP备05007700号-1
  */

class SqlLexical extends StdLexical {

  case class FloatLit(chars: String) extends Token {
    override def toString = chars
  }

  override def token: Parser[Token] =
    (identChar ~ rep(identChar | digit) ^^ { case first ~ rest => processIdent(first :: rest mkString "") }
      | rep1(digit) ~ opt('.' ~> rep(digit)) ^^ {
      case i ~ None => NumericLit(i mkString "")
      case i ~ Some(d) => FloatLit(i.mkString("") + "." + d.mkString(""))
    }
      | '\'' ~ rep(chrExcept('\'', '\n', EofCh)) ~ '\'' ^^ { case '\'' ~ chars ~ '\'' => StringLit(chars mkString "") }
      | '\"' ~ rep(chrExcept('\"', '\n', EofCh)) ~ '\"' ^^ { case '\"' ~ chars ~ '\"' => StringLit(chars mkString "") }
      | EofCh ^^^ EOF
      | '\'' ~> failure("unclosed string literal")
      | '\"' ~> failure("unclosed string literal")
      | delim
      | failure("illegal character")
      )

  def regex(r: Regex): Parser[String] = new Parser[String] {
    def apply(in: Input) = {
      val source = in.source
      val offset = in.offset
      val start = offset // handleWhiteSpace(source, offset)
      (r findPrefixMatchOf (source.subSequence(start, source.length))) match {
        case Some(matched) =>
          Success(source.subSequence(start, start + matched.end).toString,
            in.drop(start + matched.end - offset))
        case None =>
          Success("", in)
      }
    }
  }
}

