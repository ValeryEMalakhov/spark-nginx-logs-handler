package com.prb.dnhs

import scala.util._

import cats.data.Validated
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import com.prb.dnhs.entities._
import com.prb.dnhs.exceptions._
import com.prb.dnhs.parsers._

/**
  * The ExecutorContext object contains a number of parameters
  * that enable to work with application classes.
  */
object ExecutorContext extends LoggerHelper {

  private val rddStringParser: DataParser[String, Either[Exception, LogEntry]] = new DataFirstParser()
  private val logEntryParser: DataParser[LogEntry, Either[Exception, Row]] = new DataSecondParser()

  def parse(logRDD: RDD[String]): RDD[Row] = {

    logRDD.map { row =>

      val logEntry = rddStringParser.parse(row) match {
        case Left(DataException(x)) =>
          logger.warn(x)
          null
        case Right(value) => value
      }

      val logRow = logEntryParser.parse(logEntry) match {
        case Left(_) => logger.warn("Some parser error")// correct the parser previously
          null
        case Right(value) => value
      }

      logRow
    }
  }
}

