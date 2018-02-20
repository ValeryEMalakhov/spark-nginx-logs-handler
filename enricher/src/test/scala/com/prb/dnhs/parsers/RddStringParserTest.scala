package com.prb.dnhs.parsers

import com.prb.dnhs.entities.LogEntry
import com.prb.dnhs.exceptions.ErrorDetails
import com.prb.dnhs.exceptions.ErrorType._
import com.prb.dnhs.validators.{NonEmptinessValidator, Validator}
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.specs2._

class RddStringParserTest extends mutable.Specification
  with MockitoSugar {

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

  private val emptyGeneralFieldString =
    s"01/Jan/2000:00:00:01\t-\t01234567890123456789012345678901\t001" +
      s"\t127.0.0.1\t127.0.0.1\tMozilla/5.0 (Windows NT 10.0; Win64; x64)" +
      s"\tAdId=100&SomeId=012345"

  ///////////////////////////////////////////////////////////////////////////
  // An objects of the test classes
  ///////////////////////////////////////////////////////////////////////////

  private val nonEmptinessValidatorImpl = mock[NonEmptinessValidator]

  when(nonEmptinessValidatorImpl.validate(logString))
    .thenReturn(Right(logString))

  when(nonEmptinessValidatorImpl.validate(emptyGeneralFieldString))
    .thenReturn(Right(emptyGeneralFieldString))

  when(nonEmptinessValidatorImpl.validate(""))
    .thenReturn(Left(ErrorDetails(1, ParserError, "Log-string is empty!", "")))

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
      rddStringParser.parse(logString) must beRight(logEntry)
    }
    "incorrect string, it must return Either.Left with ParserError" >> {
      rddStringParser.parse(emptyGeneralFieldString)
        .left.get.errorType must_== ParserError
    }
  }
}

