package com.prb.dnhs.parsers

import com.prb.dnhs.constants.TestSparkSession
import com.prb.dnhs.entities.SerializableContainer
import com.prb.dnhs.exceptions.ErrorDetails
import com.prb.dnhs.exceptions.ErrorType.ParserError
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.scalatest.mockito.MockitoSugar
import org.mockito.Mockito._
import org.specs2.mutable

class MainParserTest extends mutable.Specification
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

  private val dataParserImpl =
    mock[DataParserImpl](withSettings.serializable)

  when(dataParserImpl.parse(""))
    .thenReturn(Left(ErrorDetails(1, ParserError, "Log-string is empty!", "")))

  when(dataParserImpl.parse("test"))
    .thenReturn(Seq(Right(Row("test"))).head)

  private val testParserContainer =
    mock[SerializableContainer[DataParser[String, Either[ErrorDetails, Row]]]](withSettings.serializable)

  when(testParserContainer.value)
    .thenReturn(dataParserImpl)

  private def mainParser = new MainParser {
    override val parser: SerializableContainer[DataParser[String, Either[ErrorDetails, Row]]] = testParserContainer
  }

  ///////////////////////////////////////////////////////////////////////////
  // Test body
  ///////////////////////////////////////////////////////////////////////////

  // get all parsed rows, including rows with errors
  def validParsedLogString: RDD[Either[ErrorDetails, Row]] = mainParser.parse(validRddLogString)

  def invalidParsedLogString: RDD[Either[ErrorDetails, Row]] = mainParser.parse(invalidRddLogString)

  "If the parser has completed successfully, and rows is valid then" >> {
    "result seq must be equal to `testLogRowSeq`" >> {
      validParsedLogString.collect must_== Seq(Right(Row("test"))).toArray
    }
  }
  "If the parser has completed successfully, and rows is invalid then" >> {
    "result seq must be equal to `invalidLogRowSeq`" >> {
      invalidParsedLogString.collect
        .must_==(Seq(Left(ErrorDetails(1, ParserError, "Log-string is empty!", ""))).toArray)
    }
  }
}

