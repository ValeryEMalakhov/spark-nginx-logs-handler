package com.prb.dnhs.parsers

import scala.language.implicitConversions

import cats.data.Validated
import com.prb.dnhs.LoggerHelper
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import com.prb.dnhs.ExecutorContext._
import com.prb.dnhs.entities._
import com.prb.dnhs.exceptions.DataException

class DataSecondParser extends DataParser[LogEntry, Either[Exception, Row]] with LoggerHelper {

  def parse(logEntry: LogEntry): Either[Exception, Row] = {

    val immutableFields = Row(
      logEntry.dateTime,
      logEntry.eventType,
      logEntry.requestId,
      logEntry.userCookie,
      logEntry.site,
      logEntry.ipAddress,
      logEntry.useragent
    )

    logger.warn(logEntry.dateTime)
    logger.warn(logEntry.eventType)
    logger.warn(LogEntry.toString)

    val mutableDataTypeArray: Array[(String, DataType, Boolean)] = schemaMap(logEntry.eventType)
      .fields
      .map(f => (f.name, f.dataType, f.nullable))
      .drop(LogEntry.getClass.getDeclaredFields.length)

    val mutableFields: Seq[Row] =
      try {
        mutableDataTypeArray.map { row =>
          row._2 match {
            case StringType => Row(logEntry.queryString(row._1).toString)
            case IntegerType => Row(logEntry.queryString(row._1).toInt)
            case ArrayType(StringType, _) => Row(logEntry.queryString(row._1).split(",").toList)
            case FloatType => Row(logEntry.queryString(row._1).toFloat)
            case DoubleType => Row(logEntry.queryString(row._1).toDouble)
            case BooleanType => Row(logEntry.queryString(row._1).toBoolean)
          }
        }
      }
      catch {
        case _: IllegalArgumentException => {
          return Left(DataException(s"Wrong data type!"))
        }
      }

    Right(mutableFields.foldLeft(immutableFields)((head: Row, tail: Row) => Row.merge(head, tail)))
  }
}

