package com.prb.dnhs.handlers

import com.prb.dnhs.entities.SerializableContainer
import com.prb.dnhs.exceptions.ErrorDetails
import com.prb.dnhs.validators.Validator
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

abstract class MainHandler extends RowHandler[RDD[Either[ErrorDetails, Row]], RDD[Row]] {

  val validRowHandler: RowHandler[RDD[Either[ErrorDetails, Row]], RDD[Row]]
  val invalidRowHandler: RowHandler[RDD[Either[ErrorDetails, Row]], Unit]

  override def handle(
      data: RDD[Either[ErrorDetails, Row]],
      outputDir: String): RDD[Row] = {

    // handle all invalid rows
    // val invalidRows = invalidRowHandler.handle(logRow, outputDir)

    // get only valid rows, which ready for recording
    validRowHandler.handle(data)
  }
}