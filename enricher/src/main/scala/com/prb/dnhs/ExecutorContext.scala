package com.prb.dnhs

import scala.util.Either

import com.prb.dnhs.entities._
import com.prb.dnhs.exceptions.DataValidationExceptions
import com.prb.dnhs.helpers.LoggerHelper
import com.prb.dnhs.parsers._
import org.apache.spark.sql.Row

/**
  * The ExecutorContext object contains a number of parameters
  * that enable to work with application classes.
  */
object ExecutorContext extends LoggerHelper {

  val schemas: SchemaRepositorуImpl = new SchemaRepositorуImpl()

  val rddStringParser: DataParser[String, Either[DataValidationExceptions, LogEntry]] = new RddStringParser()

  val logEntryParser: DataParser[Option[LogEntry], Either[Exception, Row]] = new LogEntryParser()
}

