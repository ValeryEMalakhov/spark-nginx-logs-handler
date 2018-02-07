package com.prb.dnhs.parsers

import com.prb.dnhs.constants.{TestConst, TestSparkSession}
import com.prb.dnhs.entities.{LogEntry, SchemaRepositorу, SchemaRepositoryImpl, SerializableContainer}
import com.prb.dnhs.exceptions._
import com.prb.dnhs.validators.{NonEmptinessValidator, QueryStringValidator}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.mockito.Mockito
import org.specs2.mutable

class DataParserImplTest extends mutable.Specification
  with TestSparkSession
  with TestConst {

  ///////////////////////////////////////////////////////////////////////////
  // Test values
  ///////////////////////////////////////////////////////////////////////////

  private val testRddLogString: RDD[String] = spark.sparkContext.parallelize(testLogStringSeq)

  ///////////////////////////////////////////////////////////////////////////
  // An objects of the test classes.
  ///////////////////////////////////////////////////////////////////////////

  private val dataParserImpl = Mockito.mock(classOf[DataParserImpl], Mockito.withSettings().serializable())

  Mockito.when(dataParserImpl.parse(testLogStringSeq(0))).thenReturn(Right(testLogRowSeq(0)))
  Mockito.when(dataParserImpl.parse(testLogStringSeq(1))).thenReturn(Right(testLogRowSeq(1)))
  Mockito.when(dataParserImpl.parse(testLogStringSeq(2))).thenReturn(Right(testLogRowSeq(2)))

  private val testParserContainer = Mockito.mock(
    classOf[SerializableContainer[DataParser[String, Either[ErrorDetails, Row]]]],
    Mockito.withSettings().serializable()
  )

  Mockito
    .when(testParserContainer.value)
    .thenReturn(dataParserImpl)

  ///////////////////////////////////////////////////////////////////////////
  // Test body
  ///////////////////////////////////////////////////////////////////////////

  def parse(logRDD: RDD[String]): RDD[Either[ErrorDetails, Row]] = {

    val _parser = testParserContainer

    // get all parsed rows, including rows with errors
    logRDD.map(_parser.value.parse)
  }

  private val parsedLogString: RDD[Either[ErrorDetails, Row]] = parse(testRddLogString)

  private val validParsedLogString: RDD[Row] = parsedLogString.map(_.right.get)

  "If the parser has completed successfully, then" >> {
    "in first row" >> {
      "dateTime must be equivalent to the `01/Jan/2000:00:00:01`" >> {
        validParsedLogString.collect.head(0) must_== "01/Jan/2000:00:00:01"
      }
      "segments must be equivalent to the `(111,true,some info)`" >> {
        validParsedLogString.collect.head(7) must_== List("{111", "true", "some info}")
      }
      "AdId must be null" >> {
        validParsedLogString.collect.head(8) must_== null
      }
      "SomeId must be null" >> {
        validParsedLogString.collect.head(9) must_== null
      }
    }
  }
}

