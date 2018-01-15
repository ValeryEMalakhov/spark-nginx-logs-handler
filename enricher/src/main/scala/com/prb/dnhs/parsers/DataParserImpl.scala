package com.prb.dnhs.parsers

import cats.data.Validated
import com.prb.dnhs.{DriverContext, ExecutorContext}
import com.prb.dnhs.entities.LogEntry
import com.prb.dnhs.helpers.LoggerHelper
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

class DataParserImpl
  extends DataParser[String, Option[Row]]
    with LoggerHelper {

  override def parse(row: String): Option[Row] = {

    val logEntry: Option[LogEntry] =
      ExecutorContext.rddStringParser.parse(row) match {
        case Validated.Invalid(x) =>
          logger.warn(x.getMessage)
          None
        case Validated.Valid(value) => Some(value)
      }

    ExecutorContext.logEntryParser.parse(logEntry) match {
      case Validated.Invalid(x) =>
        logger.warn(x.getMessage)
        None
      case Validated.Valid(value) => Some(value)
    }
  }
}
