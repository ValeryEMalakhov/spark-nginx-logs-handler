package com.prb.dnhs.parsers

import scala.language.implicitConversions

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.types._

import com.prb.dnhs.DriverContext._
import com.prb.dnhs.entities._
import com.prb.dnhs.exceptions._

class DataSecondParser extends DataParser[RDD[LogEntry], RDD[Row]] {

  def parse(logRDD: RDD[LogEntry]): RDD[Row] = {

    //  if log entry string is valid, then get Row with that string, else get Null
    val parsedLogRDD: RDD[Row] = logRDD.map { log =>
      if (DataSecondParser.fieldValidator(log)) {
        DataSecondParser.fieldsBuilder(log)
      }
      else null
    }

    parsedLogRDD.filter(_ != null)
  }
}

//  Necessity in the object since serialization occurs in the `map`
private object DataSecondParser {

  private def fieldValidator(log: LogEntry): Boolean = {

    val immutableFields = Row(
      log.dateTime,
      log.eventType,
      log.requesrId,
      log.userCookie,
      log.site,
      log.ipAddress,
      log.useragent
    )

    if (immutableFields.mkString(",").contains("null")) {
      //  throw DataException(s"Immutable fields must not have empty fields")
      println(s"Immutable fields must not have empty fields")
      return false
    }

    log.eventType match {
      case "rt" => {
        //  check for the presence of mutable fields
        if (!(core.columns sameElements rt.columns)) {
          val checkFields = rt.schema.fields.map(f => (f.name, f.dataType, f.nullable)).drop(core.columns.length)

          validateMutableLogColumns(checkFields, log.segments.toList) match {
            case Some(toReturn) => toReturn
            case None => true
          }
        } else true
      }
      case "impr" => {
        //  check for the presence of mutable fields
        if (!(core.columns sameElements impr.columns)) {
          val checkFields = impr.schema.fields.map(f => (f.name, f.dataType, f.nullable)).drop(core.columns.length)

          validateMutableLogColumns(checkFields, log.segments.toList) match {
            case Some(toReturn) => toReturn
            case None => true
          }
        } else true
      }
      case "clk" => {
        //  check for the presence of mutable fields
        if (!(core.columns sameElements clk.columns)) {
          val checkFields = clk.schema.fields.map(f => (f.name, f.dataType, f.nullable)).drop(core.columns.length)

          validateMutableLogColumns(checkFields, log.segments.toList) match {
            case Some(toReturn) => toReturn
            case None => true
          }
        } else true
      }
      case null => {
        //  throw new SchemaValidationException(s"Failed to find eventType.")
        println(s"Failed to find eventType.")
        false
      }
    }
  }

  private def validateMutableLogColumns(
      checkFields: Array[(String, DataType, Boolean)],
      segmentList: List[(String, String)]): Option[Boolean] = {

    checkFields.zipWithIndex.foreach { case (field, i) =>
      if (segmentList.lengthCompare(i) != 0) {
        //  first check for non-null field
        if (segmentList(i)._1 == field._1) {
          if (segmentList(i)._2 != "-") {

            //TODO: Add validation by sql.DataType

          } else {
            //  throw DataException(s"Field ${segmentList(i)._1} must not be empty")
            println(s"Field ${segmentList(i)._1} must not be empty")
            return Some(false)
          }
        } else {
          if (!field._3) {
            //  throw DataException(s"Field ${segmentList(i)._1} is not nullable")
            println(s"Field ${segmentList(i)._1} is not nullable")
            return Some(false)
          }
        }
      }
      else {
        //  throw DataException(s"Missing required fields")
        println(s"Missing required fields")
        return Some(false)
      }
    }
    None
  }

  private def fieldsBuilder(log: LogEntry) = {
    //  unchanged part of the log-data, none of the column can be Null
    val immutableFields = Row(
      log.dateTime,
      log.eventType,
      log.requesrId,
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
      case _ => immutableFields
    }
  }

  private def mutableFieldsBuilder(
      log: LogEntry,
      immutableFields: Row,
      segmentsList: Seq[(String, String)],
      dataType: Array[DataType]) = {

    val mutableFields: Array[Row] = mutablePartOfSchema.zipWithIndex
      .map {
        case (list, i) =>
          if (segmentsList.lengthCompare(i) != 0) {
            if (segmentsList(i)._1 == list) {
              dataType(i) match {
                case StringType => Row(segmentsList(i)._2.toString)
                case IntegerType => Row(segmentsList(i)._2.toInt)
                case BooleanType => Row(segmentsList(i)._2.toBoolean)
              }
            } else Row(null)
          } else Row(null)
      }

    mutableFields.foldLeft(immutableFields)((head: Row, tail: Row) => Row.merge(head, tail))
  }
}
