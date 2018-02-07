package com.prb.dnhs.parsers

import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._
import scala.language.implicitConversions

import com.prb.dnhs.entities._
import com.prb.dnhs.exceptions.ErrorDetails
import com.prb.dnhs.exceptions.ErrorType.ParserError
import com.prb.dnhs.helpers.LoggerHelper
import com.prb.dnhs.validators.Validator
import org.apache.http.client.utils.URLEncodedUtils

abstract class RddStringParser
  extends DataParser[String, Either[ErrorDetails, LogEntry]]
    with LoggerHelper {

  val nonEmptinessValidator: Validator[String, Either[ErrorDetails, String]]

  val TAB = "\t"

  override def parse(logRDD: String): Either[ErrorDetails, LogEntry] = {

    for {
      notEmptyString <- nonEmptinessValidator.validate(logRDD).right
      // breaks the inputDir string into tabs
      parsedLog <- parseRddString(logRDD).right
      // if the mutable field does not contain a hyphen, parse it and store in a Map
      queryString <- buildSegmentsMap(parsedLog(7)).right
      // build `LogEntry` using parsed string
      logEntry <- buildLogEntry(logRDD, parsedLog, queryString).right
    } yield logEntry
  }

  private def parseRddString(logRDD: String) = Right(logRDD.split(TAB))

  /**
    * Decrypts arguments from the `queryString` and transforms the result into a Map
    *
    * @param queryString arguments in the http query string
    */
  private def buildSegmentsMap(queryString: String) = {
    Right(
      URLEncodedUtils
        .parse(queryString, StandardCharsets.UTF_8)
        .asScala
        .filter(t => t.getValue != "-")
        .map(t => t.getName -> t.getValue)
        .toMap
    )
  }

  /**
    * Build a LogEntry object based on the parsed original log line,
    * if it does not contain a hyphen (the null equivalent in the logs)
    *
    * @param logRDD      original log string
    * @param parsedLog   log string after parsing (by '\t')
    * @param queryString Map based on http query string (@args)
    */
  private def buildLogEntry(logRDD: String, parsedLog: Seq[String], queryString: Map[String, String]) = {

    Either.cond(
      // if
      !parsedLog.contains("-"),
      // valid
      LogEntry(parsedLog(0), parsedLog(1), parsedLog(2), parsedLog(3),
        parsedLog(4), parsedLog(5), parsedLog(6), queryString),
      // invalid
      ErrorDetails(errorType = ParserError, errorMessage = "General field's value is empty", line = logRDD)
    )
  }
}

