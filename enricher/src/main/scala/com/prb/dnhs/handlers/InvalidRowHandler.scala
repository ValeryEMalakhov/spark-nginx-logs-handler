package com.prb.dnhs.handlers

import com.prb.dnhs.exceptions.ErrorDetails
import com.prb.dnhs.exceptions.ErrorType.OtherError
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

abstract class InvalidRowHandler extends RowHandler[RDD[Either[ErrorDetails, Row]], Unit] {

  val TAB = "\t"

  override def handle(
      data: RDD[Either[ErrorDetails, Row]],
      outputDir: String): Unit = {

    // get only invalid rows
    val logRowDefective = data.flatMap {
      case Left(err) => Some(s"${err.timestamp}$TAB${err.errorType}$TAB${err.errorMessage}$TAB${err.line}")
      case Right(_) => None
    }

    // get rows that can be re-processed
    val logRowRepeatable = reparsableLogRowSeparator(data)

    // save invalid rows
    saveInvalidRows(logRowDefective, outputDir, "DEFECTIVE")

    saveInvalidRows(logRowRepeatable, outputDir, "REPARSE")
  }

  private def reparsableLogRowSeparator(
      logRowRdd: RDD[Either[ErrorDetails, Row]]): RDD[String] = {

    logRowRdd.flatMap {
      case Left(err) => {
        if (err.errorType == OtherError)
          Some(s"${err.timestamp}$TAB${err.errorType}$TAB${err.errorMessage}$TAB${err.line}")
        else None
      }
      case Right(_) => None
    }
  }

  private def saveInvalidRows(
      logRow: RDD[String],
      outputDir: String,
      pathSpecification: String): Unit = {
    // some code
  }
}
