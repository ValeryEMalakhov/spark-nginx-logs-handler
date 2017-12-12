package com.prb.dnhs

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import com.prb.dnhs.executors._
import com.prb.dnhs.entities._
import com.prb.dnhs.parsers._

/**
  * The ExecutorContext object contains a number of parameters
  * that enable to work with application classes.
  */
object ExecutorContext {

  val converterToDataFrame: RddConverter[DataFrame] = new RddToDataFrameConverter()

  val rddParser: DataParser[RDD[String], RDD[LogEntry]] = new DataFirstParser()
  val logEntryParser: DataParser[RDD[LogEntry], RDD[Row]] = new DataSecondParser()

}

