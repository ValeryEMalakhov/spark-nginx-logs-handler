/*
package com.prb.dnhs.parsers

import com.prb.dnhs.entities.LogEntry
import com.prb.dnhs._
import com.prb.dnhs.exceptions._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.specs2._

class DataParserTest extends mutable.Specification {

  //   create local Spark config with default settings for tests
  private val specSparkConf = new SparkConf()
    .setAppName("LogsEnricher")
    .setMaster("local[2]")

  //   create local Spark context with Spark configuration for tests
  private val spcSC = new SparkContext(specSparkConf)

  private val testLogString =
    "01/Jan/2000:00:00:01\trt\t01234567890123456789012345678901\t001\t127.0.0.1\t127.0.0.1\tMozilla/5.0 (Windows NT 10.0; Win64; x64)\tsegments={111,true,some%20info}\n01/Jan/2000:00:00:03\timpr\t01234567890123456789012345678901\t001\t127.0.0.1\t127.0.0.1\tMozilla/5.0 (Windows NT 10.0; Win64; x64)\tAdId=100\n01/Jan/2000:00:00:05\tclk\t01234567890123456789012345678901\t001\t127.0.0.1\t127.0.0.1\tMozilla/5.0 (Windows NT 10.0; Win64; x64)\tAdId=101&SomeId=012345"

  val testRddLogString: RDD[String] = spcSC.parallelize(testLogString.split("\n"))

  val parsedLogString: RDD[Row] = DriverContext.localDataParser.parse(testRddLogString)

  "If the parser has completed successfully, then" >> {
    "in first row" >> {
      "dateTime must be equivalent to the `01/Jan/2000:00:00:01`" >> {
        parsedLogString.collect.head(0) must_== "01/Jan/2000:00:00:01"
      }
      "AdId must be null" >> {
        parsedLogString.collect.head(7) must_== null
      }
      "SomeId must be null" >> {
        parsedLogString.collect.head(8) must_== null
      }
    }
    "in last row" >> {
      "dateTime must be equivalent to the `01/Jan/2000:00:00:05`" >> {
        parsedLogString.collect.last(0) must_== "01/Jan/2000:00:00:05"
      }
      "AdId must be equivalent to the `101`" >> {
        parsedLogString.collect.last(7) must_== 101
      }
      "SomeId must be equivalent to the `012345`" >> {
        parsedLogString.collect.last(8) must_== 12345
      }
    }
  }
}
*/
