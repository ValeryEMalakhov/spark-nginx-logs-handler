package com.prb.dnhs.constants

import com.prb.dnhs.entities.LogEntry
import com.prb.dnhs.exceptions.ErrorDetails
import com.prb.dnhs.exceptions.ErrorType.ParserError
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

trait TestConst {

  ///////////////////////////////////////////////////////////////////////////
  // Test constants
  ///////////////////////////////////////////////////////////////////////////

  val TAB = "\t"

  val GENERIC_EVENT = "generic-event"
  val DEFAULT_SCHEMAS = Map(
    "rt" -> "rt.parquet",
    "impr" -> "impr.parquet",
    "clk" -> "clk.parquet"
  )

  ///////////////////////////////////////////////////////////////////////////
  // Test values
  ///////////////////////////////////////////////////////////////////////////

  val testSchemas: Map[String, StructType] = Map(
    "rt" -> StructType(
      StructField("dateTime", StringType, false) ::
        StructField("eventType", StringType, false) ::
        StructField("requesrId", StringType, false) ::
        StructField("userCookie", StringType, false) ::
        StructField("site", StringType, false) ::
        StructField("ipAddress", StringType, false) ::
        StructField("useragent", StringType, false) ::
        StructField("segments", ArrayType(StringType, false), false) :: Nil
    ),
    "impr" -> StructType(
      StructField("dateTime", StringType, false) ::
        StructField("eventType", StringType, false) ::
        StructField("requesrId", StringType, false) ::
        StructField("userCookie", StringType, false) ::
        StructField("site", StringType, false) ::
        StructField("ipAddress", StringType, false) ::
        StructField("useragent", StringType, false) ::
        StructField("AdId", IntegerType, true) :: Nil
    ),
    "clk" -> StructType(
      StructField("dateTime", StringType, false) ::
        StructField("eventType", StringType, false) ::
        StructField("requesrId", StringType, false) ::
        StructField("userCookie", StringType, false) ::
        StructField("site", StringType, false) ::
        StructField("ipAddress", StringType, false) ::
        StructField("useragent", StringType, false) ::
        StructField("AdId", IntegerType, true) ::
        StructField("SomeId", StringType, true) :: Nil
    )
  )

  val genericSchema = StructType(
    StructField("dateTime", StringType, false) ::
      StructField("eventType", StringType, false) ::
      StructField("requesrId", StringType, false) ::
      StructField("userCookie", StringType, false) ::
      StructField("site", StringType, false) ::
      StructField("ipAddress", StringType, false) ::
      StructField("useragent", StringType, false) ::
      StructField("segments", ArrayType(StringType, false), false) ::
      StructField("AdId", IntegerType, true) ::
      StructField("SomeId", StringType, true) :: Nil
  )

  // Valid

  val testLogString: String =
    s"01/Jan/2000:00:00:01${TAB}clk${TAB}01234567890123456789012345678901${TAB}001" +
      s"${TAB}127.0.0.1${TAB}127.0.0.1${TAB}Mozilla/5.0 (Windows NT 10.0; Win64; x64)" +
      s"${TAB}AdId=100&SomeId=012345"

  val testLogStringSeq: Seq[String] = Seq(
    s"01/Jan/2000:00:00:01${TAB}rt${TAB}01234567890123456789012345678901${TAB}001" +
      s"${TAB}127.0.0.1${TAB}127.0.0.1${TAB}Mozilla/5.0 (Windows NT 10.0; Win64; x64)" +
      s"${TAB}segments={111,true,some%20info}",
    s"01/Jan/2000:00:00:01${TAB}impr${TAB}01234567890123456789012345678901${TAB}001" +
      s"${TAB}127.0.0.1${TAB}127.0.0.1${TAB}Mozilla/5.0 (Windows NT 10.0; Win64; x64)" +
      s"${TAB}AdId=100",
    s"01/Jan/2000:00:00:01${TAB}clk${TAB}01234567890123456789012345678901${TAB}001" +
      s"${TAB}127.0.0.1${TAB}127.0.0.1${TAB}Mozilla/5.0 (Windows NT 10.0; Win64; x64)" +
      s"${TAB}AdId=100&SomeId=012345")

  val testLogEntry =
    LogEntry("01/Jan/2000:00:00:01",
      "clk",
      "01234567890123456789012345678901",
      "001",
      "127.0.0.1",
      "127.0.0.1",
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
      Map("AdId" -> "100", "SomeId" -> "012345")
    )

  val testLogRow =
    Row("01/Jan/2000:00:00:01",
      "clk",
      "01234567890123456789012345678901",
      "001",
      "127.0.0.1",
      "127.0.0.1",
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
      null,
      100,
      "012345"
    )

  val testLogRowSeq: Seq[Row] = Seq(
    Row("01/Jan/2000:00:00:01",
      "rt",
      "01234567890123456789012345678901",
      "001",
      "127.0.0.1",
      "127.0.0.1",
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
      List("{111", "true", "some info}"),
      null,
      null
    ),
    Row("01/Jan/2000:00:00:01",
      "clk",
      "01234567890123456789012345678901",
      "001",
      "127.0.0.1",
      "127.0.0.1",
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
      null,
      100,
      null
    ),
    Row("01/Jan/2000:00:00:01",
      "clk",
      "01234567890123456789012345678901",
      "001",
      "127.0.0.1",
      "127.0.0.1",
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
      null,
      100,
      "012345"
    )
  )

  val testSimpleLogRowSeq: Seq[Either[ErrorDetails, Row]] = Seq(
    Left(ErrorDetails(errorType = ParserError, errorMessage = "testInvalid", line = "")),
    Right(Row("testValid")),
    Left(ErrorDetails(errorType = ParserError, errorMessage = "testInvalid", line = ""))
  )

  // Invalid

  val emptyTLS = ""

  val emptyGeneralFieldTLS: String =
    s"01/Jan/2000:00:00:01${TAB}-${TAB}01234567890123456789012345678901${TAB}001" +
      s"${TAB}127.0.0.1${TAB}127.0.0.1${TAB}Mozilla/5.0 (Windows NT 10.0; Win64; x64)" +
      s"${TAB}AdId=100&SomeId=012345"

  val wrongQueryStringDataTypeTLE =
    LogEntry("01/Jan/2000:00:00:01",
      "clk",
      "01234567890123456789012345678901",
      "001",
      "127.0.0.1",
      "127.0.0.1",
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
      Map("AdId" -> "err", "SomeId" -> "012345")
    )

  val testInvalidSimpleLogRowSeq: Seq[Either[ErrorDetails, Row]] = Seq(
    Left(ErrorDetails(errorType = ParserError, errorMessage = "testInvalid", line = "")),
    Left(ErrorDetails(errorType = ParserError, errorMessage = "testInvalid", line = ""))
  )
}

