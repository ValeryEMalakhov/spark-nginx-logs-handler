/*
package com.prb.dnhs

import com.prb.dnhs.entities.LogEntry
import org.apache.spark.rdd.RDD
import org.specs2._
import org.apache.spark.{SparkConf, SparkContext}

class DataFirstParserTest extends mutable.Specification {

  //   create local Spark config with default settings for tests
  private val specSparkConf = new SparkConf()
    .setAppName("LogsEnricher")
    .setMaster("local[2]")

  //   create local Spark context with Spark configuration for tests
  private val spcSC = new SparkContext(specSparkConf)

  //   The creation of these Sequences made it possible to avoid calls to the file system in the tests

  //   logEntry lines equivalent to the first three lines of the logEntry file `15_09_*`
  private val testLogString_1 = Seq[String](
    "29/Nov/2017:15:05:21 +0000\trt\tb5a8a368df05b837211ac8de7aca2bfd\t100\t192.168.80.132\t192.168.80.1\tMozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36\t-", "29/Nov/2017:15:05:33 +0000\timpr\t2b8625702c5563ffff1a4f1d6c16c9f6\t100\t192.168.80.132\t192.168.80.1\tMozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36\tAdId=100", "29/Nov/2017:15:05:36 +0000\tclk\t26f770590fa1d753dc1d1e3a4d214148\t100\t192.168.80.132\t192.168.80.1\tMozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36\tAdId=100,SomeId=123446")

  //   logEntry lines equivalent to the last three lines of the logEntry file `15_12_*`
  private val testLogString_2 = Seq[String](
    "29/Nov/2017:15:12:18 +0000\timpr\tc8f79b15d2bcf6245bace205de50ead0\t105\t192.168.80.132\t192.168.80.1\tMozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36\tAdId=103", "29/Nov/2017:15:12:29 +0000\timpr\t94385342a5f0f4e871666f722d1d682c\t103\t192.168.80.132\t192.168.80.1\tMozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36\tAdId=104", "29/Nov/2017:15:12:40 +0000\tclk\t134211f6e0aae99718e47c5a69d5b6a1\t103\t192.168.80.132\t192.168.80.1\tMozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/62.0.3202.94 Safari/537.36\tAdId=104,SomeId=123xyz")

  "When RDD contains" >> {

    "single logEntry archive from test `file`" >> {

      val singleLogRDD: RDD[LogEntry] = ExecutorContext.rddParser.parse(spcSC.parallelize(testLogString_1))

      "it must contain `b5a8a368df05b837211ac8de7aca2bfd` id in first row" >> {
        //   each event line contains a unique `request_id`, so the tests can use it in comparisons
        singleLogRDD.first.requestId must_== "b5a8a368df05b837211ac8de7aca2bfd"
      }
    }

    "multiple logEntry archives from test `files`" >> {

      val logs: Seq[String] = testLogString_1 ++ testLogString_2

      val multiLogsRDD: RDD[LogEntry] = ExecutorContext.rddParser.parse(spcSC.parallelize(logs))

      "it must contain `b5a8a368df05b837211ac8de7aca2bfd` id in first row" >> {
        multiLogsRDD.first.requestId must_== "b5a8a368df05b837211ac8de7aca2bfd"
      }

      "and `134211f6e0aae99718e47c5a69d5b6a1` id in last row" >> {
        multiLogsRDD.collect.last.requestId must_== "134211f6e0aae99718e47c5a69d5b6a1"
      }
    }
  }
}

*/
