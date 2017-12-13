package com.prb.dnhs.validators

import scala.language.implicitConversions

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import com.prb.dnhs.DriverContext._
import com.prb.dnhs.exceptions._
import com.prb.dnhs.entities._

object LogRowValidator {

  implicit def fieldsValidator(log: LogEntry): Boolean = {

    val immutableFields = Row(
      log.dateTime,
      log.eventType,
      log.requesrId,
      log.userCookie,
      log.site,
      log.ipAddress,
      log.useragent
    )

    // immutable fields are enough to check for not null
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

          mutableFieldsValidator(checkFields.toList, log.segments.toList) match {
            case Some(toReturn) => toReturn
            case None => true
          }
        } else true
      }
      case "impr" => {
        //  check for the presence of mutable fields
        if (!(core.columns sameElements impr.columns)) {
          val checkFields = impr.schema.fields.map(f => (f.name, f.dataType, f.nullable)).drop(core.columns.length)

          mutableFieldsValidator(checkFields.toList, log.segments.toList) match {
            case Some(toReturn) => toReturn
            case None => true
          }
        } else true
      }
      case "clk" => {
        //  check for the presence of mutable fields
        if (!(core.columns sameElements clk.columns)) {
          val checkFields = clk.schema.fields.map(f => (f.name, f.dataType, f.nullable)).drop(core.columns.length)

          mutableFieldsValidator(checkFields.toList, log.segments.toList) match {
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

  private def mutableFieldsValidator(
      checkFields: List[(String, DataType, Boolean)],
      segmentsList: List[(String, String)]): Option[Boolean] = {

    checkFields.zipWithIndex.foreach { case (field, i) =>
      if (segmentsList.lengthCompare(i) != 0) {
        //  first check for non-null field
        if (segmentsList(i)._1 == field._1) {
          if (!segmentsList(i)._2.contains("-")) {
            try {
              checkFields(i)._2 match {
                case StringType => segmentsList(i)._2.toString
                case IntegerType => segmentsList(i)._2.toInt
                case BooleanType => segmentsList(i)._2.toBoolean
              }
            }
            catch {
              case _: IllegalArgumentException => {
                //  throw DataException(s"Wrong data type! Expected type: ${checkFields(i)._2}")
                println(s"Wrong data type! Expected type: ${checkFields(i)._2}")
                return Some(false)
              }
            }
          } else {
            //  throw DataException(s"Field ${segmentList(i)._1} must not be empty")
            println(s"Field ${segmentsList(i)._1} must not be empty")
            return Some(false)
          }
        } else {
          if (!field._3) {
            //  throw DataException(s"Field ${segmentList(i)._1} is not nullable")
            println(s"Field ${segmentsList(i)._1} is not nullable")
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
}
