package com.prb.dnhs.parsers

import com.prb.dnhs.constants.TestSparkSession
import com.prb.dnhs.entities.{LogEntry, SerializableContainer}
import com.prb.dnhs.exceptions.ErrorType.ParserError
import com.prb.dnhs.exceptions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.specs2.mutable

class DataParserImplTest extends mutable.Specification
  with TestSparkSession
  with MockitoSugar {

  ///////////////////////////////////////////////////////////////////////////
  // Test values
  ///////////////////////////////////////////////////////////////////////////

  private def validRddLogString: RDD[String] =
    sparkSession.sparkContext.parallelize(Seq("test"))

  private def invalidRddLogString: RDD[String] =
    sparkSession.sparkContext.parallelize(Seq(""))

  ///////////////////////////////////////////////////////////////////////////
  // An objects of the test classes
  ///////////////////////////////////////////////////////////////////////////

  private val dataParserImpl = mock[DataParserImpl](withSettings.serializable)

  when(dataParserImpl.parse(""))
    .thenReturn(Left(ErrorDetails(1, ParserError, "Log-string is empty!", "")))

  when(dataParserImpl.parse("test"))
    .thenReturn(Seq(Right(Row("test"))).head)

  private val testParserContainer =
    mock[SerializableContainer[DataParser[String, Either[ErrorDetails, Row]]]](withSettings.serializable)

  when(testParserContainer.value)
    .thenReturn(dataParserImpl)

  def parse(logRDD: RDD[String]): RDD[Either[ErrorDetails, Row]] = {

    val _parser = testParserContainer

    // get all parsed rows, including rows with errors
    logRDD.map(_parser.value.parse)
  }

  ///////////////////////////////////////////////////////////////////////////
  // Test body
  ///////////////////////////////////////////////////////////////////////////

  private def parsedLogString: RDD[Either[ErrorDetails, Row]] = parse(validRddLogString)

  private def invalidParsedLogString: RDD[Either[ErrorDetails, Row]] = parse(invalidRddLogString)

  "If the parser has completed successfully, and rows is valid then" >> {
    "result seq must be equal to `validLogRowSeq`" >> {
      parsedLogString.collect() must_== Seq(Right(Row("test"))).toArray
    }
  }
  "If the parser has completed successfully, and rows is invalid then" >> {
    "result seq must be equal to `invalidLogRowSeq`" >> {
      invalidParsedLogString.collect
        .must_==(Seq(Left(ErrorDetails(1, ParserError, "Log-string is empty!", ""))).toArray)
    }
  }
}

