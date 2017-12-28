package com.prb.dnhs.parsers

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import com.prb.dnhs.entities._
import com.prb.dnhs.entities.SchemaRepos._
import com.prb.dnhs.validators.LogRowValidator._
import com.prb.dnhs.ScalaLogger._

class DataSecondParser extends DataParser[RDD[LogEntry], RDD[Row]] {

  def parse(logRDD: RDD[LogEntry]): RDD[Row] = {

    // if logEntry entry string is valid, then get Row with that string, else get Null
    val parsedLogRDD: RDD[Row] = logRDD.map { logEntry =>
      if (fieldsValidator(logEntry)) {
        DataSecondParser.fieldsBuilder(logEntry)
      }
      else {
        logger.error(s"An invalid row is [${logEntry.dateTime}, ${logEntry.requestId}]")
        null
      }
    }

    parsedLogRDD.filter(_ != null)
  }
}

// Necessity in the object since serialization occurs in the `map`
private object DataSecondParser {

  private def fieldsBuilder(logEntry: LogEntry) = {
    // unchanged part of the logEntry-data, none of the column can be Null
    val immutableFields = Row(
      logEntry.dateTime,
      logEntry.eventType,
      logEntry.requestId,
      logEntry.userCookie,
      logEntry.site,
      logEntry.ipAddress,
      logEntry.useragent,
      logEntry.segments
    )

    val dataType: Array[DataType] = getSchema(logEntry.eventType).fields.map(_.dataType).drop(getSchema("core").length)

    val segmentsList = if (!(getSchema(logEntry.eventType).fields sameElements getSchema("core").fields)) {
      logEntry.mutableFields.toList
    } else List(("", ""))

    mutableFieldsBuilder(logEntry, immutableFields, segmentsList, dataType)
  }

  private def mutableFieldsBuilder(
      log: LogEntry,
      immutableFields: Row,
      segmentsList: Seq[(String, String)],
      dataType: Array[DataType]) = {

    val mutableFields: Seq[Row] = getSchema("mutable").zipWithIndex
      .map { case (list, i) =>
        if (segmentsList.lengthCompare(i) != 0) {
          if (segmentsList(i)._1 == list.name) {
            dataType(i) match {
              case StringType => Row(segmentsList(i)._2.toString)
              case IntegerType => Row(segmentsList(i)._2.toInt)
              case FloatType => Row(segmentsList(i)._2.toFloat)
              case DoubleType => Row(segmentsList(i)._2.toDouble)
              case BooleanType => Row(segmentsList(i)._2.toBoolean)
            }
          } else Row(null)
        } else Row(null)
      }

    mutableFields.foldLeft(immutableFields)((head: Row, tail: Row) => Row.merge(head, tail))
  }
}
