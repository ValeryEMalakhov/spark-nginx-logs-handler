package com.prb.dnhs.parsers

import com.prb.dnhs.entities.LogEntry
import com.prb.dnhs.exceptions.ErrorDetails
import org.apache.spark.sql.Row

abstract class DataParserImpl
  extends DataParser[String, Either[ErrorDetails, Row]] {

  val rddStringParser: DataParser[String, Either[ErrorDetails, LogEntry]]
  val logEntryParser: DataParser[LogEntry, Either[ErrorDetails, Row]]

  override def parse(inputLog: String): Either[ErrorDetails, Row] = {

    for {
      // getting LogEntry (or one of exceptions) by parsing string
      logEntry <- rddStringParser.parse(inputLog).right
      // getting Row (or one of exceptions) by parsing LogEntry
      row <- logEntryParser.parse(logEntry).right
    } yield row
  }
}

