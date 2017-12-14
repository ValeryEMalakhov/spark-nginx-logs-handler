package com.prb.dnhs.parsers

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import com.prb.dnhs.DriverContext._
import com.prb.dnhs.entities._
import com.prb.dnhs.validators.LogRowValidator._

class DataSecondParser extends DataParser[RDD[LogEntry], RDD[Row]] {

  def parse(logRDD: RDD[LogEntry]): RDD[Row] = {

    //  if logEntry entry string is valid, then get Row with that string, else get Null
    val parsedLogRDD: RDD[Row] = logRDD.map { logEntry =>
      if (fieldsValidator(logEntry)) {
        DataSecondParser.fieldsBuilder(logEntry)
      }
      else {
        log.error(s"An invalid row is [${logEntry.dateTime}, ${logEntry.requestId}]")
        null
      }
    }

    parsedLogRDD.filter(_ != null)
  }
}

//  Necessity in the object since serialization occurs in the `map`
private object DataSecondParser {

  private def fieldsBuilder(log: LogEntry) = {
    //  unchanged part of the logEntry-data, none of the column can be Null
    val immutableFields = Row(
      log.dateTime,
      log.eventType,
      log.requestId,
      log.userCookie,
      log.site,
      log.ipAddress,
      log.useragent
    )

    log.eventType match {
      case "rt" => {
        val dataType: Array[DataType] = rt.schema.fields.map(_.dataType).drop(core.columns.length)

        val segmentsList = if (!(core.columns sameElements rt.columns)) {
          log.segments.toList
        } else List(("", ""))

        mutableFieldsBuilder(log, immutableFields, segmentsList, dataType)
      }
      case "impr" => {
        val dataType = impr.schema.fields.map(_.dataType).drop(core.columns.length)

        val segmentsList = if (!(core.columns sameElements impr.columns)) {
          log.segments.toList
        } else List(("", ""))

        mutableFieldsBuilder(log, immutableFields, segmentsList, dataType)
      }
      case "clk" => {
        val dataType = clk.schema.fields.map(_.dataType).drop(core.columns.length)

        val segmentsList = if (!(core.columns sameElements clk.columns)) {
          log.segments.toList
        } else List(("", ""))

        mutableFieldsBuilder(log, immutableFields, segmentsList, dataType)
      }
      //  cannot be reached after a correct validation
      case _ => immutableFields
    }
  }

  private def mutableFieldsBuilder(
      log: LogEntry,
      immutableFields: Row,
      segmentsList: Seq[(String, String)],
      dataType: Array[DataType]) = {

    val mutableFields: Array[Row] = mutablePartOfSchema.zipWithIndex
      .map { case (list, i) =>
        if (segmentsList.lengthCompare(i) != 0) {
          if (segmentsList(i)._1 == list) {
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
