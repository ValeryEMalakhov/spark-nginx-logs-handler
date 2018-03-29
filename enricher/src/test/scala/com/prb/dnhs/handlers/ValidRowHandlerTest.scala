package com.prb.dnhs.handlers

import com.prb.dnhs.constants.TestSparkSession
import com.prb.dnhs.exceptions.ErrorDetails
import com.prb.dnhs.exceptions.ErrorType.ParserError
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.specs2.mutable

class ValidRowHandlerTest extends mutable.Specification
  with TestSparkSession {

  ///////////////////////////////////////////////////////////////////////////
  // Test values
  ///////////////////////////////////////////////////////////////////////////

  private def testRddLogString: RDD[Either[ErrorDetails, Row]] =
    sparkContext.parallelize(Seq(Right(Row("testValid"))))

  private def testInvalidRddLogString: RDD[Either[ErrorDetails, Row]] =
    sparkContext.parallelize(Seq(
      Left(ErrorDetails(errorType = ParserError, errorMessage = "testInvalid", line = ""))
    ))

  ///////////////////////////////////////////////////////////////////////////
  // An objects of the test classes.
  ///////////////////////////////////////////////////////////////////////////

  private def validRowHandler
  : RowHandler[RDD[Either[ErrorDetails, Row]], RDD[Row]] =
    new ValidRowHandler()

  ///////////////////////////////////////////////////////////////////////////
  // Test body
  ///////////////////////////////////////////////////////////////////////////

  "If the `ValidRowHandler` gets RDD with " >> {
    "valid rows, handler must return RDD with correct rows" >> {
      validRowHandler.handle(testRddLogString).collect must_== Seq(Row("testValid")).toArray
    }
    "invalid rows, handler must return nothing" >> {
      validRowHandler.handle(testInvalidRddLogString).collect must_== Array.empty[Row]
    }
  }
}

