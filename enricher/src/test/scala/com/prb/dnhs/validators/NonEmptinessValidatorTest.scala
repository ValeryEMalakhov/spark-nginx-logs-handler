package com.prb.dnhs.validators

import com.prb.dnhs.exceptions.ErrorType.ParserError
import org.specs2.mutable

class NonEmptinessValidatorTest extends mutable.Specification {

  ///////////////////////////////////////////////////////////////////////////
  // Test values
  ///////////////////////////////////////////////////////////////////////////

  private val TAB = "\t"

  private val testLogString: String =
    s"01/Jan/2000:00:00:01${TAB}clk${TAB}01234567890123456789012345678901${TAB}001" +
      s"${TAB}127.0.0.1${TAB}127.0.0.1${TAB}Mozilla/5.0 (Windows NT 10.0; Win64; x64)" +
      s"${TAB}AdId=100&SomeId=012345"

  ///////////////////////////////////////////////////////////////////////////
  // An objects of the test classes.
  ///////////////////////////////////////////////////////////////////////////

  private def validator = new NonEmptinessValidator()

  ///////////////////////////////////////////////////////////////////////////
  // Test body
  ///////////////////////////////////////////////////////////////////////////

  "If the `nonEmptinessValidator` gets" >> {
    "the correct entry line, it must return it" >> {
      validator.validate(testLogString) must beRight(testLogString)
    }
    "an invalid entry string, it must return Either.Left with ParserError" >> {
      validator.validate("").left.get.errorType must_== ParserError
    }
  }
}
