package com.prb.dnhs

import com.prb.dnhs.entities._
import com.prb.dnhs.exceptions.ErrorDetails
import com.prb.dnhs.helpers.ConfigHelper
import com.prb.dnhs.parsers._
import com.prb.dnhs.validators.{QueryStringValidator, NonEmptinessValidator}
import org.apache.spark.sql.Row

/**
  * The ExecutorContext object contains a number of parameters
  * that enable to work with application classes.
  */
object ExecutorContext extends ConfigHelper {

  private val schemasImpl: SchemaRepositorу = new SchemaRepositoryImpl()

  private val queryStringValidatorImpl = new QueryStringValidator()

  private val nonEmptinessValidatorImpl = new NonEmptinessValidator()

  val rddStringParserImpl
  : DataParser[String, Either[ErrorDetails, LogEntry]] =
    new RddStringParser() {

      lazy val nonEmptinessValidator = nonEmptinessValidatorImpl
    }

  val logEntryParserImpl
  : DataParser[LogEntry, Either[ErrorDetails, Row]] =
    new LogEntryParser() {

      lazy val schemas = schemasImpl
      lazy val queryStringValidator = queryStringValidatorImpl
    }

  val dataParserImpl
  : DataParser[String, Either[ErrorDetails, Row]] =
    new DataParserImpl() {

      lazy val rddStringParser = rddStringParserImpl
      lazy val logEntryParser = logEntryParserImpl
    }
}

