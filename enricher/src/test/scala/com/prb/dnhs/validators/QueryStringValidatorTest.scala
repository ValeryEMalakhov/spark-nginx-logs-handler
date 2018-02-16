package com.prb.dnhs.validators

import com.prb.dnhs.entities.LogEntry
import com.prb.dnhs.exceptions.ErrorType.ParserError
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.specs2.mutable

class QueryStringValidatorTest extends mutable.Specification {

  ///////////////////////////////////////////////////////////////////////////
  // Test values
  ///////////////////////////////////////////////////////////////////////////

  private val schema = StructType(
    StructField("dateTime",   StringType, false) ::
    StructField("eventType",  StringType, false) ::
    StructField("requesrId",  StringType, false) ::
    StructField("userCookie", StringType, false) ::
    StructField("site",       StringType, false) ::
    StructField("ipAddress",  StringType, false) ::
    StructField("useragent",  StringType, false) ::
    StructField("AdId",       IntegerType, true) ::
    StructField("SomeId",     StringType, true) ::
    Nil
  )

  private val testLogEntry =
    LogEntry("01/Jan/2000:00:00:01", "clk", "01234567890123456789012345678901", "001",
      "127.0.0.1", "127.0.0.1", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
      Map("AdId" -> "100", "SomeId" -> "012345")
    )

  private val wrongQueryStringDataTypeTLE =
    LogEntry("01/Jan/2000:00:00:01", "clk", "01234567890123456789012345678901", "001",
      "127.0.0.1", "127.0.0.1", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
      Map("AdId" -> "err", "SomeId" -> "012345")
    )

  ///////////////////////////////////////////////////////////////////////////
  // An objects of the test classes.
  ///////////////////////////////////////////////////////////////////////////

  private def validator = new QueryStringValidator()

  ///////////////////////////////////////////////////////////////////////////
  // Test body
  ///////////////////////////////////////////////////////////////////////////

  "If the `QueryStringValidator` gets" >> {
    "valid LogEntry, it must return Either.Right with Seq of query string's Rows" >> {
      validator.validate(
        testLogEntry, 7, schema
      ) must beRight(Seq(Row(100), Row("012345")))
    }
    "invalid LogEntry with wrong datatype, it must return Either.Left with ParserError" >> {
      validator.validate(
        wrongQueryStringDataTypeTLE, 7, schema
      ).left.get.errorType must_== ParserError
    }
  }
}

