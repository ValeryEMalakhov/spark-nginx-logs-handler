package com.prb.dnhs.parsers

import com.prb.dnhs.constants.TestSparkSession
import com.prb.dnhs.entities.SerializableContainer
import com.prb.dnhs.exceptions.ErrorDetails
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.mockito.Mockito
import org.mockito.Mockito._
import org.specs2.mutable

class MainParserTest extends mutable.Specification
  with TestSparkSession {

  ///////////////////////////////////////////////////////////////////////////
  // Test values
  ///////////////////////////////////////////////////////////////////////////

  private val TAB = "\t"

  private val testLogStringSeq: Seq[String] = Seq(
    s"01/Jan/2000:00:00:01${TAB}rt${TAB}01234567890123456789012345678901${TAB}001" +
      s"${TAB}127.0.0.1${TAB}127.0.0.1${TAB}Mozilla/5.0 (Windows NT 10.0; Win64; x64)" +
      s"${TAB}segments={111,true,some%20info}",
    s"01/Jan/2000:00:00:01${TAB}impr${TAB}01234567890123456789012345678901${TAB}001" +
      s"${TAB}127.0.0.1${TAB}127.0.0.1${TAB}Mozilla/5.0 (Windows NT 10.0; Win64; x64)" +
      s"${TAB}AdId=100",
    s"01/Jan/2000:00:00:01${TAB}clk${TAB}01234567890123456789012345678901${TAB}001" +
      s"${TAB}127.0.0.1${TAB}127.0.0.1${TAB}Mozilla/5.0 (Windows NT 10.0; Win64; x64)" +
      s"${TAB}AdId=100&SomeId=012345")

  private val testLogRowSeq: Seq[Row] = Seq(
    Row("01/Jan/2000:00:00:01", "rt", "01234567890123456789012345678901", "001",
      "127.0.0.1", "127.0.0.1", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
      List("{111", "true", "some info}"), null, null),
    Row("01/Jan/2000:00:00:01", "clk", "01234567890123456789012345678901", "001",
      "127.0.0.1", "127.0.0.1", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
      null, 100, null),
    Row("01/Jan/2000:00:00:01", "clk", "01234567890123456789012345678901", "001",
      "127.0.0.1", "127.0.0.1", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
      null, 100, "012345")
  )

  private def testRddLogString: RDD[String] = sparkSession.sparkContext.parallelize(testLogStringSeq)

  ///////////////////////////////////////////////////////////////////////////
  // An objects of the test classes
  ///////////////////////////////////////////////////////////////////////////

  private val dataParserImpl = mock(classOf[DataParserImpl], withSettings().serializable())

  when(dataParserImpl.parse(testLogStringSeq(0)))
    .thenReturn(Right(testLogRowSeq(0)))
  when(dataParserImpl.parse(testLogStringSeq(1)))
    .thenReturn(Right(testLogRowSeq(1)))
  when(dataParserImpl.parse(testLogStringSeq(2)))
    .thenReturn(Right(testLogRowSeq(2)))

  private val testParserContainer = mock(
    classOf[SerializableContainer[DataParser[String, Either[ErrorDetails, Row]]]],
    withSettings().serializable()
  )

  when(testParserContainer.value)
    .thenReturn(dataParserImpl)

  private def mainParser = new MainParser {
    override val parser: SerializableContainer[DataParser[String, Either[ErrorDetails, Row]]] = testParserContainer
  }

  ///////////////////////////////////////////////////////////////////////////
  // Test body
  ///////////////////////////////////////////////////////////////////////////

  /*
    "data parser mock test" >> {
      verify(testParserContainer, Mockito.times(4))
      verify(testParserContainer, Mockito.atLeastOnce())
      verify(testParserContainer, Mockito.never())
    }
  */

  // get all parsed rows, including rows with errors
  def parsedLogString: RDD[Either[ErrorDetails, Row]] = mainParser.parse(testRddLogString)

  def validParsedLogString: RDD[Row] = parsedLogString.map(_.right.get)

  "If the parser has completed successfully, then" >> {
    "in first row" >> {
      "dateTime must be equivalent to the `01/Jan/2000:00:00:01`" >> {
        validParsedLogString.collect.head(0) must_== "01/Jan/2000:00:00:01"
      }
      "segments must be equivalent to the `(111,true,some info)`" >> {
        validParsedLogString.collect.head(7) must_== List("{111", "true", "some info}")
      }
      "AdId must be null" >> {
        validParsedLogString.collect.head(8) must beNull
      }
      "SomeId must be null" >> {
        validParsedLogString.collect.head(9) must beNull
      }
    }
  }
}

