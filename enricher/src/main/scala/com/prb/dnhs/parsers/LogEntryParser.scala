package com.prb.dnhs.parsers

import scala.language.implicitConversions

import cats.data.Validated
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import com.prb.dnhs._
import com.prb.dnhs.entities.LogEntry
import com.prb.dnhs.exceptions.DataValidationExceptions
import com.prb.dnhs.helpers.LoggerHelper

class LogEntryParser
  extends DataParser[Option[LogEntry], Validated[DataValidationExceptions, Row]]
    with LoggerHelper {

  private val schemas = ExecutorContext.schemasImpl

  override def parse(logEntry: Option[LogEntry]): Validated[DataValidationExceptions, Row] = {

    logEntry match {
      case Some(log) => {

        val generalFields = Row(
          log.dateTime, log.eventType, log.requestId,
          log.userCookie, log.site, log.ipAddress,
          log.useragent
        )

        val mutableDataTypeArray: Array[(String, DataType, Boolean)] =
          getDataTypeArray(generalFields) match {
            case Some(toReturn) => toReturn
            case None => return Validated.Invalid(exceptions.UnexpectedEventType)
          }

        val mutableFields: Seq[Row] =
          getMutableFieldsSeq(mutableDataTypeArray, log) match {
            case Some(toReturn) => toReturn
            case None => return Validated.Invalid(exceptions.MutableFieldIsEmpty)
          }

        Validated.Valid(mutableFields.foldLeft(generalFields)((head: Row, tail: Row) => Row.merge(head, tail)))
      }
      case None => Validated.Invalid(exceptions.MutableFieldIsEmpty)
    }
  }

  private def getDataTypeArray(generalFields: Row): Option[Array[(String, DataType, Boolean)]] = {

    schemas.getSchema("generic-event") match {
      case Some(schema) => {
        Some(schema
          .fields
          .map(f => (f.name, f.dataType, f.nullable))
          .drop(generalFields.length))
      }
      case None => None
    }
  }

  private def getMutableFieldsSeq(
      mutableDataTypeArray: Array[(String, DataType, Boolean)],
      log: LogEntry): Option[Seq[Row]] = {

    try {
      Some(mutableDataTypeArray.map { row =>
        if (log.queryString.exists(_._1 == row._1))
          Row(row._2 match {
            case StringType => log.queryString(row._1).toString
            case IntegerType => log.queryString(row._1).toInt
            case ArrayType(StringType, _) => log.queryString(row._1).split(",").toList
          })
        else
          Row(null)
      })
    }
    catch {
      case _: IllegalArgumentException => None
    }
  }
}

