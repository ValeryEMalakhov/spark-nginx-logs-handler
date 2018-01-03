package com.prb.dnhs.parsers

import scala.language.implicitConversions
import cats.data.Validated

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import com.prb.dnhs._
import com.prb.dnhs.entities.LogEntry
import com.prb.dnhs.helpers.LoggerHelper

class LogEntryParser
  extends DataParser[Option[LogEntry], Either[Exception, Row]]
    with LoggerHelper {

  override def parse(logEntry: Option[LogEntry]): Either[Exception, Row] = {

    logEntry match {
      case Some(log) => {

        val immutableFields = Row(
          log.dateTime, log.eventType, log.requestId,
          log.userCookie, log.site, log.ipAddress,
          log.useragent
        )

        val mutableDataTypeArray: Array[(String, DataType, Boolean)] =
          ExecutorContext.schemas.getSchema("generic-event") match {
            case Some(schema) => schema
              .fields
              .map(f => (f.name, f.dataType, f.nullable))
              .drop(immutableFields.length)
            case None => return Left(exceptions.UnexpectedEventType)
          }

        val mutableFields: Seq[Row] =
          try {
            mutableDataTypeArray.map { row =>
              if (log.queryString.exists(_._1 == row._1))
                Row(row._2 match {
                  case StringType => log.queryString(row._1).toString
                  case IntegerType => log.queryString(row._1).toInt
                  case ArrayType(StringType, _) => log.queryString(row._1).split(",").toList
                })
              else
                Row(null)
            }
          }
          catch {
            case _: IllegalArgumentException => {
              return Left(exceptions.IncorrectDataTypeExceptions)
            }
          }

        Right(mutableFields.foldLeft(immutableFields)((head: Row, tail: Row) => Row.merge(head, tail)))
      }
      case None => Left(exceptions.MutableFieldIsEmpty)
    }
  }
}

