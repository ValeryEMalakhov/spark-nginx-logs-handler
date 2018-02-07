package com.prb.dnhs.parsers

import com.prb.dnhs.constants.TestConst
import com.prb.dnhs.entities.{LogEntry, SchemaRepositoryImpl, SchemaRepositorу}
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

  private val queryStringValidatorImpl = new QueryStringValidator()

  private val logEntryParser
  : DataParser[LogEntry, Either[ErrorDetails, Row]] =
    new LogEntryParser() {

      lazy val schemas: SchemaRepositorу = schemasImpl
      lazy val queryStringValidator: QueryStringValidator = queryStringValidatorImpl
    }

  ///////////////////////////////////////////////////////////////////////////
  // Test body
  ///////////////////////////////////////////////////////////////////////////

  "If the `LogEntryParser` gets" >> {
    // valid
    "correct string, it must return Either.Right with Row" >> {

      val res = logEntryParser.parse(testLogEntry) match {
        case Left(err) => null
        case Right(value) => value
      }

      res must_== testLogRow
    }
    // invalid
    "incorrect string, it must return Either.Left with ParserError" >> {

      val res = logEntryParser.parse(wrongQueryStringDataTypeTLE) match {
        case Left(err) => err.errorType
        case Right(value) => null
      }

      res must_== ParserError
    }
  }
}
