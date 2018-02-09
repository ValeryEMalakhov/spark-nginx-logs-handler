package com.prb.dnhs.parsers

import com.prb.dnhs.entities.LogEntry
import com.prb.dnhs.exceptions.ErrorDetails
import com.prb.dnhs.exceptions.ErrorType._
import com.prb.dnhs.validators.{NonEmptinessValidator, Validator}
import org.specs2._

class RddStringParserTest extends mutable.Specification {

  ///////////////////////////////////////////////////////////////////////////
  // Test values
  ///////////////////////////////////////////////////////////////////////////

  private val TAB = "\t"

  private val testLogString: String =
    s"01/Jan/2000:00:00:01${TAB}clk${TAB}01234567890123456789012345678901${TAB}001" +
      s"${TAB}127.0.0.1${TAB}127.0.0.1${TAB}Mozilla/5.0 (Windows NT 10.0; Win64; x64)" +
      s"${TAB}AdId=100&SomeId=012345"

  private val testLogEntry =
    LogEntry("01/Jan/2000:00:00:01", "clk", "01234567890123456789012345678901", "001",
      "127.0.0.1", "127.0.0.1", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
      Map("AdId" -> "100", "SomeId" -> "012345")
    )

  private val emptyGeneralFieldTLS: String =
    s"01/Jan/2000:00:00:01${TAB}-${TAB}01234567890123456789012345678901${TAB}001" +
      s"${TAB}127.0.0.1${TAB}127.0.0.1${TAB}Mozilla/5.0 (Windows NT 10.0; Win64; x64)" +
      s"${TAB}AdId=100&SomeId=012345"

  ///////////////////////////////////////////////////////////////////////////
  // An objects of the test classes
  ///////////////////////////////////////////////////////////////////////////

  private def nonEmptinessValidatorImpl = new NonEmptinessValidator()

  private def rddStringParser
  : DataParser[String, Either[ErrorDetails, LogEntry]] =
    new RddStringParser() {

      lazy val nonEmptinessValidator: Validator[String, Either[ErrorDetails, String]]
      = nonEmptinessValidatorImpl
    }

  ///////////////////////////////////////////////////////////////////////////
  // Test body
  ///////////////////////////////////////////////////////////////////////////

  "If the `RddStringParser` gets" >> {
    "correct string , it must return Either.Right with LogEntry object" >> {
      rddStringParser.parse(testLogString) must beRight(testLogEntry)
    }
    "incorrect string, it must return Either.Left with ParserError" >> {
      rddStringParser.parse(emptyGeneralFieldTLS).left.get.errorType must_== ParserError
    }
  }
}

