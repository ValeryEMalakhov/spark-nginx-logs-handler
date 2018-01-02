package com.prb.dnhs.parsers

import scala.util.{Either, Left, Right}

import com.prb.dnhs.entities.LogEntry
import com.prb.dnhs.helpers.LoggerHelper
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object DataParserImpl
  extends DataParser[RDD[String], RDD[Row]]
    with LoggerHelper {

  val rddStringParser: DataParser[String, Either[Exception, LogEntry]] = new RddStringParser()
  val logEntryParser: DataParser[Option[LogEntry], Either[Exception, Row]] = new LogEntryParser()

  override def parse(logRDD: RDD[String]): RDD[Row] = {

    logRDD.flatMap { row =>

      val logEntry: Option[LogEntry] = rddStringParser.parse(row) match {
        case Left(x) =>
          logger.warn(x.getMessage)
          None
        case Right(value) => Some(value)
      }

      logEntryParser.parse(logEntry) match {
        case Left(x) =>
          logger.warn(x.getMessage)
          None
        case Right(value) => Some(value)
      }
    }
  }
}
