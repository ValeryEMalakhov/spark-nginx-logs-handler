package com.prb.dnhs.handlers

import com.prb.dnhs.constants.{TestConst, TestSparkSession}
import com.prb.dnhs.exceptions.ErrorDetails
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.specs2.mutable

class ValidRowHandlerTest extends mutable.Specification
  with TestSparkSession
  with TestConst {

  ///////////////////////////////////////////////////////////////////////////
  // Test values
  ///////////////////////////////////////////////////////////////////////////

  private val testRowSeqRes: Seq[Row] = Seq(Row("testValid"))

  private val testRddLogString: RDD[Either[ErrorDetails, Row]] =
    spark.sparkContext.parallelize(testSimpleLogRowSeq)

  private val testInvalidRddLogString: RDD[Either[ErrorDetails, Row]] =
    spark.sparkContext.parallelize(testInvalidSimpleLogRowSeq)

  private val testRddLogStringRes: RDD[Row] = spark.sparkContext.parallelize(testRowSeqRes)

  ///////////////////////////////////////////////////////////////////////////
  // An objects of the test classes.
  ///////////////////////////////////////////////////////////////////////////

  private val validRowHandler
  : RowHandler[RDD[Either[ErrorDetails, Row]], RDD[Row]] =
    new ValidRowHandler()

  ///////////////////////////////////////////////////////////////////////////
  // Test body
  ///////////////////////////////////////////////////////////////////////////

  "If the `ValidRowHandler` gets test RDD with " >> {
    // valid
    "valid rows, handler must return it" >> {

      val res = validRowHandler.handle(testRddLogString)

      res.collect must_== testRddLogStringRes.collect
    }
    // invalid
    "invalid rows, handler must return nothing" >> {

      val res = validRowHandler.handle(testInvalidRddLogString)

      res.collect must_== Array.empty[Row]
    }
  }
}

