package com.prb.dnhs.validators

import com.prb.dnhs.constants.TestConst
import com.prb.dnhs.exceptions.ErrorType.ParserError
import org.apache.spark.sql.Row
import org.specs2.mutable

class QueryStringValidatorTest extends mutable.Specification
  with TestConst {

  ///////////////////////////////////////////////////////////////////////////
  // Test values
  ///////////////////////////////////////////////////////////////////////////

  private val expecterRow = Seq(Row(100), Row("012345"))

  ///////////////////////////////////////////////////////////////////////////
  // An objects of the test classes.
  ///////////////////////////////////////////////////////////////////////////

  private val validator = new QueryStringValidator()

  ///////////////////////////////////////////////////////////////////////////
  // Test body
  ///////////////////////////////////////////////////////////////////////////

  "If the `QueryStringValidator` gets" >> {
    // valid
    "valid LogEntry, it must return Either.Right with Seq of query string's Rows" >> {

      val res = validator.validate(
        testLogEntry,
        7,
        testSchemas(testLogEntry.eventType)
      ).right.get

      res must_== expecterRow
    }
    // invalid
    "invalid LogEntry with wrong datatype, it must return Either.Left with ParserError" >> {

      val res = validator.validate(
        wrongQueryStringDataTypeTLE,
        7,
        testSchemas(wrongQueryStringDataTypeTLE.eventType)
      ).left.get

      res.errorType must_== ParserError
    }
  }
}
