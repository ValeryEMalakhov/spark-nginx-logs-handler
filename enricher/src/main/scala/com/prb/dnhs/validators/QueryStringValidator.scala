package com.prb.dnhs.validators

// import scalaz.Scalaz._

import scala.util.control.NonFatal

import com.prb.dnhs.entities.LogEntry
import com.prb.dnhs.exceptions.ErrorDetails
import com.prb.dnhs.exceptions.ErrorType._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class QueryStringValidator {

  val TAB = "\t"

  def validate(
      logEntry: LogEntry,
      logEntrySize: Int,
      eventSchema: StructType): Either[ErrorDetails, Seq[Row]] = {

    val queryStringSchema = eventSchema.fields.drop(logEntrySize)

    for {
      // check that all declared fields are available
      existedRows <- validateFieldsExistence(logEntry, queryStringSchema).right
      // check the fields for the declared data types and try to convert to these types
      validRows <- validateDataType(logEntry, queryStringSchema).right
    } yield validRows
  }

  private def validateFieldsExistence(
      logEntry: LogEntry,
      schema: Seq[StructField]) = {

    val validatedRows = schema.map { field =>
      Either.cond(
        logEntry.queryString.exists(_._1 == field.name) || field.nullable,
        field.name /* -> logEntry.queryString(field.name)*/ ,
        ErrorDetails(
          errorType = ParserError,
          errorMessage = s"Schema field ${field.name} is missing",
          line = buildErrString(logEntry)
        )
      )
    }

    // Seq to Either converter
    reverseEitherSeq[String](validatedRows)
  }

  private def validateDataType(
      logEntry: LogEntry,
      schema: Seq[StructField]) = {

    try {
      Right(schema.map { field =>
        if (logEntry.queryString.exists(_._1 == field.name)) {
          Row(convertFieldValue(logEntry, field))
        } else Row(null)
      })
    }
    catch {
      case NonFatal(ex) => Left(
        ErrorDetails(
          errorType = ParserError,
          errorMessage = s"Another type of data was expected ${ex.getLocalizedMessage.toLowerCase}",
          line = buildErrString(logEntry)
        )
      )
    }
  }

  /**
    * Converts the value of the input field to the type specified by the schema
    *
    * @param logEntry the current parsing string
    * @param field    a field which value must be converted to the specified type
    */
  private def convertFieldValue(logEntry: LogEntry, field: StructField) = {
    field.dataType match {
      case StringType => logEntry.queryString(field.name).toString
      case IntegerType => logEntry.queryString(field.name).toInt
      case FloatType => logEntry.queryString(field.name).toFloat
      case ArrayType(StringType, _) => logEntry.queryString(field.name).split(",").toList
    }
  }

  /**
    * Returns the current parse string to the original format
    *
    * @param logEntry the current parse string
    */
  private def buildErrString(logEntry: LogEntry) = {

    val generalFields =
      Seq(logEntry.logDateTime,
        logEntry.eventType,
        logEntry.requestId,
        logEntry.userCookie,
        logEntry.site,
        logEntry.ipAddress,
        logEntry.useragent).mkString(TAB)

    val queryString =
      logEntry
        .queryString
        .mkString("&")
        .replaceAll(" -> ", "=")

    s"$generalFields$TAB$queryString"
  }

  /**
    * Converts the sequence of Either results into
    * one Either with a sequence of results
    *
    * @param res Seq[ Either[ErrorDetails, (String, String)] ]
    * @return Either[ ErrorDetails, Seq[(String, String)] ]
    */
  private def reverseEitherSeq[T](res: Seq[Either[ErrorDetails, T]]) = {
    // Seq[Either[ErrorDetails, (String, String)]]
    // -> Either[ErrorDetails, Seq[(String, String)]]
    // scala converter
    res
      .foldLeft(Right(Nil): Either[ErrorDetails, List[T]]) {
        (e, acc) => for (xs <- acc.right; x <- e.right) yield xs :: x
      }

    //  res
    //    .collectFirst { case Left(err) => err }
    //    .getOrElse(
    //      res
    //        .collect { case Right(value) => value }
    //    )

    // scalaz converter
    //  res.toList.sequenceU
  }
}

