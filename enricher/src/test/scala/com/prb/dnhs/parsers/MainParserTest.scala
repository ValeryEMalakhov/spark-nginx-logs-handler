package com.prb.dnhs.parsers

import com.prb.dnhs.constants.TestSparkSession
import com.prb.dnhs.entities.SerializableContainer
import com.prb.dnhs.exceptions.ErrorDetails
import com.prb.dnhs.exceptions.ErrorType.ParserError
import org.apache.spark.sql.Row
import org.mockito.Mockito._
import org.scalatest.mockito.MockitoSugar
import org.specs2.mutable

class MainParserTest extends mutable.Specification
  with TestSparkSession
  with MockitoSugar {

  ///////////////////////////////////////////////////////////////////////////
  // An objects of the test classes
  ///////////////////////////////////////////////////////////////////////////

  private val dataParserImpl =
    mock[DataParserImpl](withSettings.serializable)

  when(dataParserImpl.parse("test"))
    .thenReturn(Right(Row("test")))

  when(dataParserImpl.parse(""))
    .thenReturn(Left(ErrorDetails(1, ParserError, "Log-string is empty!", "")))

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

  "If the parser has completed successfully, and rows is valid then" >> {
    "result seq must contain an Either.Right with input string in Row" >> {
      mainParser
        .parse(sparkSession.sparkContext.parallelize(Seq("test")))
        .collect
        .must_==(Seq(Right(Row("test"))).toArray)
    }
  }
  "If the parser has completed successfully, and rows is invalid then" >> {
    "result seq must contain an Either.Left with a `ParserError`" >> {
      mainParser
        .parse(sparkSession.sparkContext.parallelize(Seq("")))
        .collect
        .must_==(Seq(Left(ErrorDetails(1, ParserError, "Log-string is empty!", ""))).toArray)
    }
  }
}

