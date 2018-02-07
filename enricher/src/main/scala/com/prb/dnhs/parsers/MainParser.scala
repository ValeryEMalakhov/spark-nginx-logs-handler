package com.prb.dnhs.parsers

import com.prb.dnhs.entities.SerializableContainer
import com.prb.dnhs.exceptions.ErrorDetails
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

abstract class MainParser extends DataParser[RDD[String], RDD[Either[ErrorDetails, Row]]] {

  val parser: SerializableContainer[DataParser[String, Either[ErrorDetails, Row]]]

  override def parse(logRDD: RDD[String]): RDD[Either[ErrorDetails, Row]] = {

    val _parser = parser

    // get all parsed rows, including rows with errors
    logRDD.map(_parser.value.parse)
  }
}
