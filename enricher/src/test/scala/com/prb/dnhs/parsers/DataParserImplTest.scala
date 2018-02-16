package com.prb.dnhs.parsers

import com.prb.dnhs.entities.{LogEntry, SchemaRepositoryImpl, SchemaRepositorу}
import com.prb.dnhs.exceptions._
import com.prb.dnhs.validators.{NonEmptinessValidator, QueryStringValidator}
import org.apache.spark.sql.Row
import org.specs2.mutable

class DataParserImplTest extends mutable.Specification {

  ///////////////////////////////////////////////////////////////////////////
  // Test values
  ///////////////////////////////////////////////////////////////////////////

  private val logString =
    s"01/Jan/2000:00:00:01\tclk\t01234567890123456789012345678901\t001" +
      s"\t127.0.0.1\t127.0.0.1\tMozilla/5.0 (Windows NT 10.0; Win64; x64)" +
      s"\tAdId=100&SomeId=012345"

  private val logEntry =
    LogEntry("01/Jan/2000:00:00:01", "clk", "01234567890123456789012345678901", "001",
      "127.0.0.1", "127.0.0.1", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
      Map("AdId" -> "100", "SomeId" -> "012345")
    )

  private val logRow =
    Row("01/Jan/2000:00:00:01", "clk", "01234567890123456789012345678901", "001",
      "127.0.0.1", "127.0.0.1", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
      null, 100, "012345"
    )

  ///////////////////////////////////////////////////////////////////////////
  // An objects of the test classes
  ///////////////////////////////////////////////////////////////////////////

  private val schemasImpl: SchemaRepositorу = new SchemaRepositoryImpl()

  private val queryStringValidatorImpl = new QueryStringValidator()

  private val nonEmptinessValidatorImpl = new NonEmptinessValidator()

  private val rddStringParserImpl
  : DataParser[String, Either[ErrorDetails, LogEntry]] =
    new RddStringParser() {

      lazy val nonEmptinessValidator = nonEmptinessValidatorImpl
    }

  private val logEntryParserImpl
  : DataParser[LogEntry, Either[ErrorDetails, Row]] =
    new LogEntryParser() {

      lazy val schemas = schemasImpl
      lazy val queryStringValidator = queryStringValidatorImpl
    }

  private val dataParserImpl
  : DataParser[String, Either[ErrorDetails, Row]] =
    new DataParserImpl() {

      lazy val rddStringParser = rddStringParserImpl
      lazy val logEntryParser = logEntryParserImpl
    }

  ///////////////////////////////////////////////////////////////////////////
  // Test body
  ///////////////////////////////////////////////////////////////////////////

  "If the parser has completed successfully, and rows is valid then" >> {
    "result Row must be equal to `logRow`" >> {
      dataParserImpl.parse(logString) must beRight(logRow)
    }
  }
  "If the parser has completed successfully, and rows is invalid then" >> {
    "result exception message must be equal to `empty` excemtion" >> {
      dataParserImpl.parse("").left.get.errorMessage must_== "Log-string is empty!"
    }
  }
}

