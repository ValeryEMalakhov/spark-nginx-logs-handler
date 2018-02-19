package com.prb.dnhs.validators

import com.prb.dnhs.exceptions.ErrorDetails
import com.prb.dnhs.exceptions.ErrorType._
import org.apache.spark.sql.Row

abstract class SaveAbilityValidatorImpl extends Validator[Row, Either[ErrorDetails, Row]] {

  // val table: DataFrame
  // val clearTable: Seq[Row]
  val userCookies: String

  override def validate(inputRow: Row): Either[ErrorDetails, Row] = {

    // if row with `rt`-event existed and have the same userCookie - save data
    val rowUserCookie = inputRow.getAs[String]("userCookie")

    if (userCookies.contains(rowUserCookie))
      Right(inputRow)
    else
      Left(ErrorDetails(
        errorType = OtherError,
        errorMessage = s"Row ${inputRow.getAs[String]("requestId")} can not be written now.",
        line = inputRow.mkString("\t"))
      )

    // if table is empty (or not existed) we can save only `rt` events
    /*
      case None => {
        if (row.getAs[String]("eventType") == "rt")
          Right(row)
        else
          Left(ErrorDetails(
            errorType = OtherError,
            errorMessage = s"Row ${row.getAs[String]("requestId")} can not be written now.",
            line = row.mkString("\t"))
          )
      }
    */
  }
}

