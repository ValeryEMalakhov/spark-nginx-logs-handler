package com.prb.dnhs.parsers

import com.prb.dnhs.constants.TestConst
import com.prb.dnhs.entities.{LogEntry, SchemaRepositoryImpl}
import com.prb.dnhs.exceptions.ErrorDetails
import com.prb.dnhs.exceptions.ErrorType._
import com.prb.dnhs.validators.QueryStringValidator
import org.apache.spark.sql.Row
import org.specs2.mutable

class LogEntryParserTest extends mutable.Specification
  with TestConst {

  ///////////////////////////////////////////////////////////////////////////
  // An objects of the test classes
  ///////////////////////////////////////////////////////////////////////////

  private val schemasImpl = new SchemaRepositoryImpl()

  private val mutableFieldsValidatorImpl = new QueryStringValidator()

  private val logEntryParser
  : DataParser[LogEntry, Either[ErrorDetails, Row]] =
    new LogEntryParser() {

      lazy val schemas = schemasImpl
      lazy val queryStringValidator = mutableFieldsValidatorImpl
    }

  ///////////////////////////////////////////////////////////////////////////
  // Test body
  ///////////////////////////////////////////////////////////////////////////

  "If" >> {
    "correct string set in `LogEntryParser`, it must return" >> {
      "LogEntry object in Either.Right" >> {
        val res = logEntryParser.parse(testLogEntry) match {
          case Left(err) => null
          case Right(value) => value
        }

        res must_== testLogRow
      }
    }
    "incorrect string set in `LogEntryParser`, it must return" >> {
      "Exception in Either.Left" >> {
        val res = logEntryParser.parse(wrongQueryStringDataTypeTLE) match {
          case Left(err) => err.errorType
          case Right(value) => null
        }

        res must_== ParserError
      }
    }
  }
}
