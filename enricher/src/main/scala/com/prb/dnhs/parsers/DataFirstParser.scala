package com.prb.dnhs.parsers

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD

import com.prb.dnhs.entities.LogEntry

class DataFirstParser extends DataParser[RDD[String], RDD[LogEntry]] {

  override def parse(logRDD: RDD[String]): RDD[LogEntry] = {

    logRDD.map { str =>
      //  breaks the input string into tabs
      val logEntry: Array[String] = str.split('\t')

      //  if the argument field does not contain a hyphen, parse it and store in a Map
      val segments: Map[String, String] =
        if (logEntry(7) != "-")
          logEntry(7).split(",").map(_.split("=")).map(pair => (pair(0), pair(1))).toMap
        else null

      LogEntry(
        DataFirstParser.hyphenToNullConverter(logEntry(0)),
        DataFirstParser.hyphenToNullConverter(logEntry(1)),
        DataFirstParser.hyphenToNullConverter(logEntry(2)),
        DataFirstParser.hyphenToNullConverter(logEntry(3)),
        DataFirstParser.hyphenToNullConverter(logEntry(4)),
        DataFirstParser.hyphenToNullConverter(logEntry(5)),
        DataFirstParser.hyphenToNullConverter(logEntry(6)),
        segments)
    }
  }
}

private object DataFirstParser {
  /**
    * Since there are no empty fields in the logs (hyphens instead of them),
    * the method replaces the dashes with Null values.
    */
  private def hyphenToNullConverter(logPart: String): String = {
    if (logPart != "-") logPart else null
  }
}
