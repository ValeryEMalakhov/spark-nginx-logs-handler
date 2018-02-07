package com.prb.dnhs.parsers

import com.prb.dnhs.constants.TestConst
import com.prb.dnhs.entities.LogEntry
import com.prb.dnhs.exceptions.ErrorDetails
import com.prb.dnhs.exceptions.ErrorType._
import com.prb.dnhs.validators.{NonEmptinessValidator, Validator}
import org.specs2._

class RddStringParserTest extends mutable.Specification
  with TestConst {

  ///////////////////////////////////////////////////////////////////////////
  // An objects of the test classes
  ///////////////////////////////////////////////////////////////////////////

  private val nonEmptinessValidatorImpl = new NonEmptinessValidator()

  private val rddStringParser
  : DataParser[String, Either[ErrorDetails, LogEntry]] =
    new RddStringParser() {

      lazy val nonEmptinessValidator: Validator[String, Either[ErrorDetails, String]]
      = nonEmptinessValidatorImpl
    }

  ///////////////////////////////////////////////////////////////////////////
  // Test body
  ///////////////////////////////////////////////////////////////////////////

  "If the `RddStringParser` gets" >> {
    // valid
    "correct string , it must return Either.Right with LogEntry object" >> {

      val res = rddStringParser.parse(testLogString) match {
        case Left(err) => null
        case Right(value) => value
      }

      res must_== testLogEntry
    }
    // invalid
    "incorrect string, it must return Either.Left with ParserError" >> {

      val res = rddStringParser.parse(emptyGeneralFieldTLS) match {
        case Left(err) => err.errorType
        case Right(value) => null
      }

      res must_== ParserError
    }
  }
}

