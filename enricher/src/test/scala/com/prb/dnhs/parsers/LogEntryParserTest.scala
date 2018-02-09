package com.prb.dnhs.parsers

import com.prb.dnhs.entities.{LogEntry, SchemaRepositoryImpl, SchemaRepositorу}
import com.prb.dnhs.exceptions.ErrorDetails
import com.prb.dnhs.exceptions.ErrorType._
import com.prb.dnhs.validators.QueryStringValidator
import org.apache.spark.sql.Row
import org.specs2.mutable

class LogEntryParserTest extends mutable.Specification {

  ///////////////////////////////////////////////////////////////////////////
  // Test values
  ///////////////////////////////////////////////////////////////////////////

  private val testLogEntry =
    LogEntry("01/Jan/2000:00:00:01", "clk", "01234567890123456789012345678901", "001",
      "127.0.0.1", "127.0.0.1", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
      Map("AdId" -> "100", "SomeId" -> "012345")
    )

  private val testLogRow =
    Row("01/Jan/2000:00:00:01", "clk", "01234567890123456789012345678901", "001",
      "127.0.0.1", "127.0.0.1", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
      null, 100, "012345"
    )

  private val wrongQueryStringDataTypeTLE =
    LogEntry("01/Jan/2000:00:00:01", "clk", "01234567890123456789012345678901", "001",
      "127.0.0.1", "127.0.0.1", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
      Map("AdId" -> "err", "SomeId" -> "012345")
    )

  ///////////////////////////////////////////////////////////////////////////
  // An objects of the test classes
  ///////////////////////////////////////////////////////////////////////////

  private def schemasImpl = new SchemaRepositoryImpl()

  private def queryStringValidatorImpl = new QueryStringValidator()

  private def logEntryParser
  : DataParser[LogEntry, Either[ErrorDetails, Row]] =
    new LogEntryParser() {

      lazy val schemas: SchemaRepositorу = schemasImpl
      lazy val queryStringValidator: QueryStringValidator = queryStringValidatorImpl
    }

  ///////////////////////////////////////////////////////////////////////////
  // Test body
  ///////////////////////////////////////////////////////////////////////////

  "If the `LogEntryParser` gets" >> {
    "correct string, it must return Either.Right with Row" >> {
      logEntryParser.parse(testLogEntry) must beRight(testLogRow)
    }
    "incorrect string, it must return Either.Left with ParserError" >> {
      logEntryParser.parse(wrongQueryStringDataTypeTLE)
        .left.get.errorType must_== ParserError
    }
  }
}
