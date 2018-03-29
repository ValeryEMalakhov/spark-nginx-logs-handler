package com.prb.dnhs.parsers

import com.prb.dnhs.entities.{LogEntry, SchemaRepository}
import com.prb.dnhs.exceptions.ErrorDetails
import com.prb.dnhs.exceptions.ErrorType._
import com.prb.dnhs.validators.QueryStringValidator
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.specs2.mutable

class LogEntryParserTest extends mutable.Specification
  with MockitoSugar {

  ///////////////////////////////////////////////////////////////////////////
  // Test values
  ///////////////////////////////////////////////////////////////////////////

  private val schema = StructType(
    StructField("dateTime", StringType, false) ::
      StructField("eventType", StringType, false) ::
      StructField("requesrId", StringType, false) ::
      StructField("userCookie", StringType, false) ::
      StructField("site", StringType, false) ::
      StructField("ipAddress", StringType, false) ::
      StructField("useragent", StringType, false) ::
      StructField("AdId", IntegerType, true) ::
      StructField("SomeId", StringType, true) ::
      Nil
  )

  private val genericSchema = StructType(
    StructField("dateTime", StringType, false) ::
      StructField("eventType", StringType, false) ::
      StructField("requesrId", StringType, false) ::
      StructField("userCookie", StringType, false) ::
      StructField("site", StringType, false) ::
      StructField("ipAddress", StringType, false) ::
      StructField("useragent", StringType, false) ::
      StructField("segments", StringType, true) ::
      StructField("AdId", IntegerType, true) ::
      StructField("SomeId", StringType, true) ::
      Nil
  )

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

  private val wrongDataTypeString =
    LogEntry("01/Jan/2000:00:00:01", "clk", "01234567890123456789012345678901", "001",
      "127.0.0.1", "127.0.0.1", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
      Map("AdId" -> "err", "SomeId" -> "012345")
    )

  ///////////////////////////////////////////////////////////////////////////
  // An objects of the test classes
  ///////////////////////////////////////////////////////////////////////////

  private val schemaImpl = mock[SchemaRepository]

  when(schemaImpl.getSchema("clk"))
    .thenReturn(Some(schema))

  when(schemaImpl.getSchema("generic-event"))
    .thenReturn(Some(genericSchema))

  when(schemaImpl.getSchema("err"))
    .thenReturn(None)

  private val queryStringValidatorImpl = mock[QueryStringValidator]

  when(queryStringValidatorImpl.validate(logEntry, 7, schema))
    .thenReturn(Right(Seq(Row(100), Row("012345"))))

  when(queryStringValidatorImpl.validate(wrongDataTypeString, 7, schema))
    .thenReturn(Left(ErrorDetails(1, ParserError, "Another type of data was expected", "Error")))

  private def logEntryParser
  : DataParser[LogEntry, Either[ErrorDetails, Row]] =
    new LogEntryParser() {

      lazy val schemas: SchemaRepository = schemaImpl
      lazy val queryStringValidator: QueryStringValidator = queryStringValidatorImpl
    }

  ///////////////////////////////////////////////////////////////////////////
  // Test body
  ///////////////////////////////////////////////////////////////////////////

  "If the `LogEntryParser` gets" >> {
    "correct string, it must return Either.Right with Row" >> {
      logEntryParser.parse(logEntry) must beRight(logRow)
    }
    "incorrect string, it must return Either.Left with ParserError" >> {
      logEntryParser.parse(wrongDataTypeString)
        .left.get.errorType must_== ParserError
    }
  }
}

