package com.prb.dnhs.parsers

import scala.util.{Either, Left, Right}

import com.prb.dnhs.ExecutorContext
import com.prb.dnhs.entities.LogEntry
import com.prb.dnhs.exceptions.DataValidationExceptions
import com.prb.dnhs.helpers.LoggerHelper
import com.prb.dnhs.validators._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object DataParserImpl
  extends DataParser[RDD[String], RDD[Row]]
    with LoggerHelper {

  override def parse(logRDD: RDD[String]): RDD[Row] = {

    logRDD.flatMap { row =>

      val logEntry: Option[LogEntry] = ExecutorContext.rddStringParser.parse(row) match {
        case Left(x) =>
          logger.warn(x.getMessage)
          None
        case Right(value) => Some(value)
      }

      ExecutorContext.logEntryParser.parse(logEntry) match {
        case Left(x) =>
          logger.warn(x.getMessage)
          None
        case Right(value) => Some(value)
      }
    }
  }
}
