/*package com.prb.dnhs.validators

import scala.language.implicitConversions

import cats.data.Validated
import cats.data.Validated._

import com.prb.dnhs.helpers.LoggerHelper
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import com.prb.dnhs.entities.SchemaRepos._
import com.prb.dnhs.exceptions._
import com.prb.dnhs.entities._*/

//trait Validator[T] {
//  def validate(in: T): Validated[T]
//}

/*object LogRowValidator extends LoggerHelper {

  implicit def fieldsValidator(logEntry: LogEntry): Validated[LogEntry, Boolean] = {

    val immutableFields = Row(
      logEntry.dateTime,
      logEntry.eventType,
      logEntry.requestId,
      logEntry.userCookie,
      logEntry.site,
      logEntry.ipAddress,
      logEntry.useragent
    )

    getSchema("core")
      .fields.map(f => f.nullable)
      .zipWithIndex
      .foreach { case (field, i) =>
        if (immutableFields.toSeq(i) == null) {
          if (!field) {
            // throw DataException(s"Immutable fields must not have empty fields")
            logger.error(s"Immutable fields must not have empty fields")
            return Invalid(logEntry, false)
          }
        }
      }

    // at the moment there are definitely only three types of events
    logEntry.eventType match {
      case "rt" =>
      case "impr" =>
      case "clk" =>
      case _ =>
        // throw new SchemaValidationException(s"Failed to find eventType.")
        logger.warn(s"Failed to find eventType.")
        return false
    }

    // check for the presence of mutable fields
    if (!(getSchema(logEntry.eventType).fields sameElements getSchema("core").fields)) {
      val checkFields = getSchema(logEntry.eventType)
        .fields.map(f => (f.name, f.dataType, f.nullable))
        .drop(getSchema("core").length)

      mutableFieldsValidator(checkFields.toList, logEntry.queryString.toList) match {
        case Some(toReturn) => toReturn
        case None => true
      }
    } else true
  }

  private def mutableFieldsValidator(
      checkFields: List[(String, DataType, Boolean)],
      segmentsList: List[(String, String)]): Option[Boolean] = {

    checkFields.zipWithIndex.foreach { case (field, i) =>
      if (segmentsList.lengthCompare(i) != 0) {
        // first check for non-null field
        if (segmentsList(i)._1 == field._1) {
          /**
            * if the field is present in the arguments, but the value is "-",
            * the field is assumed to be empty
            */
          if (!segmentsList(i)._2.contains("-")) {
            try {
              checkFields(i)._2 match {
                case StringType => segmentsList(i)._2.toString
                case IntegerType => segmentsList(i)._2.toInt
                case FloatType => segmentsList(i)._2.toFloat
                case DoubleType => segmentsList(i)._2.toDouble
                case BooleanType => segmentsList(i)._2.toBoolean
              }
            }
            catch {
              case _: IllegalArgumentException => {
                // throw DataException(s"Wrong data type! Expected type: ${checkFields(i)._2}")
                logger.warn(s"Wrong data type! Expected type: ${checkFields(i)._2}")
                return Some(false)
              }
            }
          } else {
            // throw DataException(s"Field ${segmentsList(i)._1} must not be empty")
            logger.warn(s"Field ${segmentsList(i)._1} must not be empty")
            return Some(false)
          }
        } else {
          // show an error if the field can not be empty
          if (!field._3) {
            // throw DataException(s"Field ${segmentsList(i)._1} is not nullable")
            logger.warn(s"Field ${segmentsList(i)._1} is not nullable")
            return Some(false)
          }
        }
      }
      else {
        // throw DataException(s"Missing required fields")
        logger.warn(s"Missing required fields")
        return Some(false)
      }
    }
    // return `None` (errors) if validation successfully complete
    None
  }
}*/

