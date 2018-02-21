package com.prb.dnhs.processor

import com.prb.dnhs.exceptions.ErrorDetails
import com.prb.dnhs.handlers.{FileSystemHandler, RowHandler}
import com.prb.dnhs.parsers.DataParser
import com.prb.dnhs.readers.DataReader
import com.prb.dnhs.recorders.DataRecorder
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.slf4j.Logger

abstract class Processor {

  val log: Logger
  val fsHandler: FileSystemHandler[String]
  val gzReader: DataReader[RDD[String]]
  val parser: DataParser[RDD[String], RDD[Either[ErrorDetails, Row]]]
  val handler: RowHandler[RDD[Either[ErrorDetails, Row]], RDD[Row]]
  val hiveRecorder: DataRecorder[RDD[Row]]
  val fsCleaner: FileSystemHandler[Unit]

  def process(args: ProcessorConfig): Unit = {

    log.debug("Preliminary check of working folder")
    val batchId = fsHandler.handle()

    log.debug("Reading of log-files from the file system started")
    val logRDD = gzReader.read(args.inputDir)
    log.debug("Reading of log-files from the file system is over")
    if (args.debug) printData(logRDD)

    log.debug("Parsing of log files started")
    val logRow = parser.parse(logRDD)
    log.debug("Parsing of log files is over")
    if (args.debug) printData(logRow)

    log.debug("The selection of successful results started")
    val validRow = handler.handle(logRow, batchId, args.outputDir)
    log.debug("The selection of successful results is over")
    if (args.debug) printData(validRow)

    // log.debug("Record of results in the file system started")
    // hiveRecorder.save(validRow, batchId)
    // log.debug("Record of results in the file system is over")

    // log.debug("End of processing - cleaning up the working folder")
    // fsCleaner.handle()
  }

  private def printData[T](data: RDD[T]) = {
    data.take(50).foreach(println)
  }
}

