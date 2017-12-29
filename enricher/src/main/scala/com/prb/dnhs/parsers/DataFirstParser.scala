package com.prb.dnhs.parsers

import scala.language.implicitConversions

import com.prb.dnhs.LoggerHelper
import com.prb.dnhs.entities._
import com.prb.dnhs.exceptions.DataException

class DataFirstParser extends DataParser[String, Either[Exception, LogEntry]] with LoggerHelper {

  override def parse(logRDD: String): Either[Exception, LogEntry] = {

    // breaks the input string into tabs
    val logEntry: Array[String] = logRDD.split('\t')

    // if the argument field does not contain a hyphen, parse it and store in a Map
    val queryString: Map[String, String] =
      if (logEntry(7) != "-")
        logEntry(7).split("&").map(_.split("=")).map(pair => (pair(0), pair(1))).toMap
      else Map.empty

    logEntry.foreach { row =>
      if (row == "-") return Left(DataException(s"Immutable fields must not have empty fields"))
    }

    Right(LogEntry(
      logEntry(0), logEntry(1), logEntry(2), logEntry(3),
      logEntry(4), logEntry(5), logEntry(6), queryString))
  }
}

