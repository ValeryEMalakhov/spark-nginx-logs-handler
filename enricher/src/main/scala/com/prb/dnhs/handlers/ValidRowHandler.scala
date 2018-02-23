package com.prb.dnhs.handlers

import com.prb.dnhs.exceptions.ErrorDetails
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

class ValidRowHandler extends RowHandler[RDD[Either[ErrorDetails, Row]], RDD[Row]] {

  override def handle(
      data: RDD[Either[ErrorDetails, Row]],
      outputDir: String): RDD[Row] = {

    data.filter(_.isRight).map(_.right.getOrElse(throw new RuntimeException("500")))
  }
}
