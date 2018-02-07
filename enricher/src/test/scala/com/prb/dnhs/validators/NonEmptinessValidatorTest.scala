package com.prb.dnhs.validators

import com.prb.dnhs.constants.TestConst
import com.prb.dnhs.exceptions.ErrorType.ParserError
import org.specs2.mutable

class NonEmptinessValidatorTest extends mutable.Specification
  with TestConst {

  ///////////////////////////////////////////////////////////////////////////
  // An objects of the test classes.
  ///////////////////////////////////////////////////////////////////////////

  private val validarot = new NonEmptinessValidator()

  ///////////////////////////////////////////////////////////////////////////
  // Test body
  ///////////////////////////////////////////////////////////////////////////

  "If the `nonEmptinessValidator` gets" >> {
    // valid
    "the correct entry line, it must return it." >> {

      val res = validarot.validate(testLogString) match {
        case Left(err) => null
        case Right(value) => value
      }

      res must_== testLogString
    }
    // invalid
    "an invalid entry string, it must return Either.Left with ParserError" >> {

      val res = validarot.validate(emptyTLS) match {
        case Left(err) => err.errorType
        case Right(value) => null
      }

      res must_== ParserError
    }
  }
}
