package com.prb.dnhs.parsers

import com.prb.dnhs.entities.LogEntry
import com.prb.dnhs._
import com.prb.dnhs.exceptions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.specs2._

class RddStringParserTest extends mutable.Specification {

  private val testLogString =
    "01/Jan/2000:00:00:05\tclk\t01234567890123456789012345678901\t001\t127.0.0.1\t127.0.0.1\tMozilla/5.0 (Windows NT 10.0; Win64; x64)\tAdId=101&SomeId=012345"

  private val incorrectTestLogString =
    "01/Jan/2000:00:00:05\t-\t01234567890123456789012345678901\t001\t127.0.0.1\t127.0.0.1\tMozilla/5.0 (Windows NT 10.0; Win64; x64)\tAdId=101&SomeId=012345"

  private val testLogEntry =
    LogEntry("01/Jan/2000:00:00:05",
      "clk",
      "01234567890123456789012345678901",
      "001",
      "127.0.0.1",
      "127.0.0.1",
      "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
      Map("AdId" -> "101", "SomeId" -> "012345")
    )

  "If" >> {
    "correct string set in `RddStringParser`, it must return" >> {
      "LogEntry object in Either(Right)" >> {
        ExecutorContext.rddStringParser.parse(testLogString).getOrElse() must_== testLogEntry
      }
    }
    "incorrect string set in `RddStringParser`, it must return" >> {
      "Exception in Either(Left)" >> {
        ExecutorContext.rddStringParser.parse(incorrectTestLogString).getOrElse() must_== GeneralFieldIsEmpty
      }
    }
  }
}
