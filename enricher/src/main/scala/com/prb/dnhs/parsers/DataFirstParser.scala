package com.prb.dnhs.parsers

import scala.language.implicitConversions

import com.prb.dnhs.entities.LogEntry
import org.apache.spark.rdd.RDD

class DataFirstParser extends DataParser[RDD[String], RDD[LogEntry]] {

  override def parse(logRDD: RDD[String]): RDD[LogEntry] = {

    logRDD.map { str =>
      // breaks the input string into tabs
      val logEntry = str.split('\t')

      val segments =
        if (logEntry(7) != "-")
          logEntry(7).split(",").map(_.split("=")).map(pair => (pair(0), pair(1))).toMap
        else Map("-" -> "-")

      LogEntry(logEntry(0), logEntry(1), logEntry(2), logEntry(3),
        logEntry(4), logEntry(5), logEntry(6),
        segments)
    }
  }
}
