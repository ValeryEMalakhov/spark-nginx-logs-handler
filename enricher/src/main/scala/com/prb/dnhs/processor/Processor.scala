package com.prb.dnhs.processor

import com.prb.dnhs.exceptions.ErrorDetails
import com.prb.dnhs.handlers.RowHandler
import com.prb.dnhs.parsers.DataParser
import com.prb.dnhs.readers.DataReader
import com.prb.dnhs.recorders.DataRecorder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

abstract class Processor {

  val gzReader: DataReader[RDD[String]]
  val parser: DataParser[RDD[String], RDD[Either[ErrorDetails, Row]]]
  val handler: RowHandler[RDD[Either[ErrorDetails, Row]], RDD[Row]]
  val hiveRecorder: DataRecorder[RDD[Row]]

  def process(args: ProcessorConfig): Unit = {

    // get logs from files
    val logRDD = gzReader.read(args.inputDir)
    if (args.debug) log(logRDD)

    // get parsed rows
    val logRow = parser.parse(logRDD)
    if (args.debug) log(logRow)

    // get only valid rows, which ready for recording
    val validRow = handler.handle(logRow, args.outputDir)
    if (args.debug) log(validRow)

    // save valid rows with DB updating
    // hiveRecorder.save(validRow)
  }

  private def log[T](data: RDD[T]) = {
    data.take(50).foreach(println)
  }
}

