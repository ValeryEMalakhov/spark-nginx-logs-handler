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

  private val testSimpleLogRowSeq: Seq[Either[ErrorDetails, Row]] = Seq(
    Left(ErrorDetails(errorType = ParserError, errorMessage = "testInvalid", line = "")),
    Right(Row("testValid")),
    Left(ErrorDetails(errorType = ParserError, errorMessage = "testInvalid", line = ""))
  )

  private val testInvalidSimpleLogRowSeq: Seq[Either[ErrorDetails, Row]] = Seq(
    Left(ErrorDetails(errorType = ParserError, errorMessage = "testInvalid", line = "")),
    Left(ErrorDetails(errorType = ParserError, errorMessage = "testInvalid", line = ""))
  )

  private val testRowSeqRes: Seq[Row] = Seq(Row("testValid"))

  private def testRddLogString: RDD[Either[ErrorDetails, Row]] =
    sparkSession.sparkContext.parallelize(testSimpleLogRowSeq)

  private def testInvalidRddLogString: RDD[Either[ErrorDetails, Row]] =
    sparkSession.sparkContext.parallelize(testInvalidSimpleLogRowSeq)

  private def testRddLogStringRes: RDD[Row] =
    sparkSession.sparkContext.parallelize(testRowSeqRes)

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
      validRowHandler.handle(testRddLogString).collect must_== testRowSeqRes.toArray
    }
    "invalid rows, handler must return nothing" >> {
      validRowHandler.handle(testInvalidRddLogString).collect must_== Array.empty[Row]
    }
  }
}

