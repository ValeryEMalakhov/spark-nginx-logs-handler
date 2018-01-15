package com.prb.dnhs

import cats.data.Validated
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

  val schemasImpl: SchemaRepositorуImpl = new SchemaRepositorуImpl()

  val dataParserImpl: DataParser[String, Option[Row]] = new DataParserImpl()

  val rddStringParser: DataParser[String, Validated[DataValidationExceptions, LogEntry]] = new RddStringParser()

  val logEntryParser: DataParser[Option[LogEntry], Validated[DataValidationExceptions, Row]] = new LogEntryParser()
}

