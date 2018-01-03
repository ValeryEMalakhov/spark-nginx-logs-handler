package com.prb.dnhs.validators

import scala.language.implicitConversions

import cats.data.Validated
import cats.data.Validated._

import com.prb.dnhs.helpers.LoggerHelper
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

import com.prb.dnhs.exceptions._
import com.prb.dnhs.entities._

trait LogRowValidator[T] {
  def validate(in: T): Validated[DataValidationExceptions, T]
}

object LogRowValidator extends LogRowValidator[Map[String, String]] with LoggerHelper {

  def validate(queryString: Map[String, String]): Validated[DataValidationExceptions, Map[String, String]] = {


    valid(queryString)
  }

  /*  private def mutableFieldsValidator(
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
    }*/
}

