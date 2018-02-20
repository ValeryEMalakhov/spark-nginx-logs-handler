package com.prb.dnhs.processor

import com.prb.dnhs.exceptions.ErrorDetails
import com.prb.dnhs.handlers.RowHandler
import com.prb.dnhs.parsers.DataParser
import com.prb.dnhs.readers.DataReader
import com.prb.dnhs.recorders.DataRecorder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.slf4j.Logger

abstract class Processor {

  val log: Logger
  val gzReader: DataReader[RDD[String]]
  val parser: DataParser[RDD[String], RDD[Either[ErrorDetails, Row]]]
  val handler: RowHandler[RDD[Either[ErrorDetails, Row]], RDD[Row]]
  val hiveRecorder: DataRecorder[RDD[Row]]

  def process(args: ProcessorConfig): Unit = {

    // get logs from files
    log.info("Reading of log-files from the file system started")
    val logRDD = gzReader.read(args.inputDir)
    log.info("Reading of log-files from the file system is over")
    if (args.debug) printData(logRDD)

    // get parsed rows
    log.info("Parsing of log files started")
    val logRow = parser.parse(logRDD)
    log.info("Parsing of log files is over")
    if (args.debug) printData(logRow)

    // get only valid rows, which ready for recording
    log.info("The selection of successful results started")
    val validRow = handler.handle(logRow, args.outputDir)
    log.info("The selection of successful results is over")
    if (args.debug) printData(validRow)

    // save valid rows with DB updating
    // log.info("Record of results in the file system started")
    // hiveRecorder.save(validRow)
    // log.info("Record of results in the file system is over")
  }

  private def printData[T](data: RDD[T]) = {
    data.take(50).foreach(println)
  }
}

