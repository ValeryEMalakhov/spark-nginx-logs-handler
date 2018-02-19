package com.prb.dnhs.handlers

import com.prb.dnhs.entities.SerializableContainer
import com.prb.dnhs.exceptions.ErrorDetails
import com.prb.dnhs.validators.Validator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

abstract class MainHandler extends RowHandler[RDD[Either[ErrorDetails, Row]], RDD[Row]] {

  val saveValidator: SerializableContainer[Validator[Row, Either[ErrorDetails, Row]]]
  val validRowHandler: RowHandler[RDD[Either[ErrorDetails, Row]], RDD[Row]]
  val invalidRowHandler: RowHandler[RDD[Either[ErrorDetails, Row]], Unit]

  override def handle(
      data: RDD[Either[ErrorDetails, Row]],
      outputDir: String): RDD[Row] = {

    val _saveValidator = saveValidator

    val logRow = data.map {
      case Left(err) => Left(err)
      // check for the ability to write to the database
      case Right(value) => _saveValidator.value.validate(value)
    }

    // handle all invalid rows
    // val invalidRows = invalidRowHandler.handle(outputDir, logRow)

    // get only valid rows, which ready for recording
    validRowHandler.handle(logRow)
  }
}