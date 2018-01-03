package com.prb.dnhs.parsers

import scala.language.implicitConversions

import com.prb.dnhs.entities._
import com.prb.dnhs.exceptions.DataValidationExceptions
import com.prb.dnhs.helpers.LoggerHelper
import com.prb.dnhs.validators.NonEmptyLogEntryValidator

class RddStringParser
  extends DataParser[String, Either[DataValidationExceptions, LogEntry]]
    with LoggerHelper {

  override def parse(logRDD: String): Either[DataValidationExceptions, LogEntry] = {

    // breaks the input string into tabs
    val logEntry: Array[String] = logRDD.split('\t')

    // if the mutable field does not contain a hyphen, parse it and store in a Map
    val queryString: Map[String, String] =
      if (logEntry(7) != "-")
        logEntry(7).split("&").map(_.split("=")).map(pair => (pair(0), pair(1))).toMap
      else Map.empty

    NonEmptyLogEntryValidator.validate(logEntry(0), logEntry(1), logEntry(2), logEntry(3),
      logEntry(4), logEntry(5), logEntry(6), queryString)
  }
}

