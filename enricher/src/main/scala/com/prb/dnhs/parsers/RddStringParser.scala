package com.prb.dnhs.parsers

import scala.language.implicitConversions

import cats.data.Validated
import com.prb.dnhs.entities._
import com.prb.dnhs.exceptions.{DataValidationExceptions, GeneralFieldIsEmpty}
import com.prb.dnhs.helpers.LoggerHelper

class RddStringParser
  extends DataParser[String, Validated[DataValidationExceptions, LogEntry]]
    with LoggerHelper {

  override def parse(logRDD: String): Validated[DataValidationExceptions, LogEntry] = {

    // breaks the inputDir string into tabs
    val logEntry: Array[String] = logRDD.split('\t')

    // if the mutable field does not contain a hyphen, parse it and store in a Map
    val queryString: Map[String, String] =
      if (logEntry(7) != "-")
        logEntry(7).split("&").map(_.split("=")).map(pair => (pair(0), pair(1))).toMap
      else Map.empty

    if (logEntry.contains("-")) return Validated.Invalid(GeneralFieldIsEmpty)

    Validated.Valid(LogEntry(logEntry(0), logEntry(1), logEntry(2), logEntry(3),
      logEntry(4), logEntry(5), logEntry(6), queryString))
  }
}

